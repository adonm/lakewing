use std::sync::Arc;

use arrow::array::{Array, BinaryArray, Int64Array, StringArray};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{FlightDescriptor, Ticket};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use poem::{Endpoint, Request};
use serde_json::Value;
use tonic::Code;

use crate::app::{App, Limits};
use crate::flight::FlightServer;

struct Fixture {
    _dir: tempfile::TempDir,
    batch: RecordBatch,
    wkb: String,
    geo: String,
    plain: String,
}

impl Fixture {
    async fn new() -> Self {
        static LOGGING: std::sync::Once = std::sync::Once::new();
        LOGGING.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::ERROR)
                .try_init();
        });
        // Fixture creation builds three datasets plus BTREE/RTREE indexes;
        // Lance's in-process memory reservations are shared, so concurrent
        // builds exhaust the pool (observed ExternalSorterMerge failures).
        // Serialize construction across tests.
        static BUILD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _build = BUILD.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let db = duckdb::Connection::open_in_memory().unwrap();
        db.execute_batch("INSTALL spatial; LOAD spatial").unwrap();
        let mut stmt = db
            .prepare(
                "WITH source(id, source_id, wkt) AS (VALUES
            ('c', 3::BIGINT, 'POLYGON ((4 4,5 4,5 5,4 5,4 4))'),
            ('a', 2, 'POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))'),
            ('b', 2, 'POLYGON ((4 4,5 4,5 5,4 5,4 4))'),
            ('c', 2, 'MULTIPOLYGON (((5 5,6 5,6 6,5 6,5 5)))'),
            ('d', 2, NULL)),
            geo AS (SELECT *, ST_GeomFromText(wkt) AS g FROM source)
            SELECT id, 'buildings' AS layer, source_id, ST_AsWKB(g) AS geom,
                json_object('source', source_id, 'nullable', NULL)::VARCHAR AS properties,
                ST_XMin(g) AS xmin, ST_YMin(g) AS ymin, ST_XMax(g) AS xmax, ST_YMax(g) AS ymax,
                42.5::DOUBLE AS score FROM geo",
            )
            .unwrap();
        let batch = stmt.query_arrow([]).unwrap().next().unwrap();
        let wkb = dir.path().join("wkb.lance").to_string_lossy().into_owned();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            &wkb,
            None,
        )
        .await
        .unwrap();
        let source = dir.path().join("source.parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&source).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let geo = dir.path().join("geo.lance").to_string_lossy().into_owned();
        crate::build::build(crate::build::BuildConfig {
            source: source.to_string_lossy().into_owned(),
            out: geo.clone(),
            tag: "prod".into(),
            max_rows_per_file: 3,
            max_bytes_per_file: 1024 * 1024,
        })
        .await
        .unwrap();
        let plain_batch = batch.project(&[0, 1, 2, 4, 9]).unwrap();
        let plain = dir
            .path()
            .join("plain.lance")
            .to_string_lossy()
            .into_owned();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(plain_batch.clone())], plain_batch.schema()),
            &plain,
            None,
        )
        .await
        .unwrap();
        Self {
            _dir: dir,
            batch,
            wkb,
            geo,
            plain,
        }
    }

    async fn app(&self, uri: &str, concurrency: usize) -> Arc<App> {
        self.app_with_limits(
            uri,
            Limits {
                concurrency,
                ..Default::default()
            },
        )
        .await
    }

    async fn app_with_limits(&self, uri: &str, limits: Limits) -> Arc<App> {
        let (tag, version) = if uri == self.geo {
            (Some("prod".into()), None)
        } else {
            (None, Some(1))
        };
        Arc::new(
            App::open_at(uri.into(), tag, version, Default::default(), None, limits)
                .await
                .unwrap(),
        )
    }
}

async fn get(ep: &impl Endpoint, path: &str) -> (u16, Value) {
    let response = ep
        .get_response(Request::builder().uri(path.parse().unwrap()).finish())
        .await;
    let status = response.status().as_u16();
    let body = response.into_body().into_string().await.unwrap();
    (
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_pages_sources_cursors_and_http_validation() {
    let fixture = Fixture::new().await;
    for uri in [&fixture.wkb, &fixture.geo] {
        let app = fixture.app(uri, 4).await;
        let ep = crate::api::routes(app.clone());
        // 'a' overlaps this bbox but its polygon hole contains the entire query:
        // exact intersection must drop it, and the cursor walk must never emit it.
        let mut path = "/collections/buildings/items?sources=2,3&bbox=4,4,6,6&limit=1".to_string();
        let mut rows = Vec::new();
        loop {
            let (status, body) = get(&ep, &path).await;
            assert_eq!(status, 200, "{body}");
            for row in body["features"].as_array().unwrap() {
                rows.push((
                    row["id"].as_str().unwrap().to_string(),
                    row["properties"]["source"].as_i64().unwrap(),
                    row["geometry"]["type"].as_str().unwrap().to_string(),
                ));
            }
            let next = body["links"]
                .as_array()
                .unwrap()
                .iter()
                .find(|l| l["rel"] == "next");
            match next {
                Some(next) => path = next["href"].as_str().unwrap().to_string(),
                None => break,
            }
            assert!(rows.len() <= 3, "cursor must advance");
        }
        assert_eq!(
            rows,
            vec![
                ("b".into(), 2, "Polygon".into()),
                ("c".into(), 2, "MultiPolygon".into()),
                ("c".into(), 3, "Polygon".into()),
            ]
        );
        let (status, empty) = get(&ep, "/collections/buildings/items?sources=").await;
        assert_eq!(status, 200);
        assert_eq!(empty["numberReturned"], 0);
        for query in [
            "bbox=1,2,bad,3,4",
            "bbox=NaN,0,1,1",
            "bbox=2,0,1,1",
            "limit=0",
            "offset=100001",
            "sources=2,bad",
            "unexpected=1",
        ] {
            assert_eq!(
                get(&ep, &format!("/collections/buildings/items?{query}"))
                    .await
                    .0,
                400,
                "{query}"
            );
        }
        assert_eq!(
            get(&ep, "/collections/buildings/items?snapshot=99999")
                .await
                .0,
            409
        );
        assert_eq!(get(&ep, "/collections/missing/items").await.0, 404);
        let (status, body) = get(&ep, "/collections/buildings/items/d?sources=2").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["geometry"], Value::Null);
        assert_eq!(
            body["properties"],
            serde_json::json!({"source": 2, "nullable": null})
        );

        let mut path = "/collections/buildings/items?sources=2,3&limit=1".to_string();
        let mut rows = Vec::new();
        loop {
            let (status, body) = get(&ep, &path).await;
            assert_eq!(status, 200, "{body}");
            for row in body["features"].as_array().unwrap() {
                rows.push((
                    row["id"].as_str().unwrap().to_string(),
                    row["properties"]["source"].as_i64().unwrap(),
                ));
            }
            let next = body["links"]
                .as_array()
                .unwrap()
                .iter()
                .find(|l| l["rel"] == "next");
            match next {
                Some(next) => path = next["href"].as_str().unwrap().to_string(),
                None => break,
            }
            assert!(rows.len() <= 5, "cursor must advance");
        }
        assert_eq!(
            rows,
            vec![
                ("a".into(), 2),
                ("b".into(), 2),
                ("c".into(), 2),
                ("c".into(), 3),
                ("d".into(), 2)
            ]
        );
        assert!(app.metrics.exposition().contains("lakewing_in_flight 0\n"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pages_are_isolated() {
    let fixture = Fixture::new().await;
    let app = fixture.app(&fixture.geo, 4).await;
    let ep = Arc::new(crate::api::routes(app.clone()));
    futures::stream::iter(0..80)
        .map(|n| {
            let ep = ep.clone();
            async move {
                let source = if n % 2 == 0 { 2 } else { 3 };
                let path = format!("/collections/buildings/items/c?sources={source}");
                let (status, body) = get(ep.as_ref(), &path).await;
                assert_eq!(status, 200, "{body}");
                assert_eq!(body["properties"]["source"], source);
                assert_eq!(
                    body["geometry"]["type"],
                    if source == 2 {
                        "MultiPolygon"
                    } else {
                        "Polygon"
                    }
                );
            }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    assert!(app.metrics.exposition().contains("lakewing_in_flight 0\n"));
}

/// The payload materialization must fail closed: a page whose Arrow bytes
/// exceed the 64 MiB budget returns 413 instead of loading into RAM, while
/// small windows (and single items) from the same fat collection succeed.
/// This pins the memory contract of the whole read path — pages are
/// proportional to the selection window, never the collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_budget_fails_closed() {
    static LOGGING: std::sync::Once = std::sync::Once::new();
    LOGGING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .try_init();
    });
    let dir = tempfile::tempdir().unwrap();
    // 80 rows x ~1 MiB JSON properties: a full-collection page is > 64 MiB.
    let fat = format!("{{\"padding\":\"{}\"}}", "x".repeat(1024 * 1024));
    let ids = (0..80).map(|i| format!("row{i:03}")).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("layer", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("source_id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("properties", arrow::datatypes::DataType::Utf8, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(ids)),
            Arc::new(arrow::array::StringArray::from(vec![
                "buildings".to_string();
                80
            ])),
            Arc::new(arrow::array::Int64Array::from(vec![2i64; 80])),
            Arc::new(arrow::array::StringArray::from(vec![Some(fat.clone()); 80])),
        ],
    )
    .unwrap();
    let uri = dir.path().join("fat.lance").to_string_lossy().into_owned();
    let schema = batch.schema();
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        &uri,
        None,
    )
    .await
    .unwrap();
    let app = Arc::new(
        crate::app::App::open_at(
            uri.clone(),
            None,
            Some(1),
            Default::default(),
            None,
            Limits {
                concurrency: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let ep = crate::api::routes(app.clone());

    // The full page must refuse past the budget, not materialize.
    let (status, body) = get(&ep, "/collections/buildings/items?sources=2&limit=10000").await;
    assert_eq!(status, 413, "{body}");
    assert_eq!(body["code"], 413);

    // A bounded window over the same collection is fine, and the failed
    // request must not have stranded an admission permit or a worker.
    let (status, body) = get(&ep, "/collections/buildings/items?sources=2&limit=3").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["numberReturned"], 3);
    let (status, body) = get(&ep, "/collections/buildings/items/row000?sources=2").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["id"], "row000");
    assert!(app.metrics.exposition().contains("lakewing_in_flight 0\n"));
    assert!(app
        .metrics
        .exposition()
        .contains("lakewing_http_responses_total{status=\"413\"} 1\n"));
}

/// Warm repeats answer from the rendered-response cache: identical bytes,
/// conditional requests still honor the exact ETag, distinct effective
/// sources never collide, and hits neither hold admission nor re-render.
/// With the cache disabled the same requests still succeed (all misses).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_cache_serves_repeats_and_isolates_sources() {
    let fixture = Fixture::new().await;
    for cache_bytes in [64 * 1024 * 1024usize, 0] {
        let app = fixture
            .app_with_limits(&fixture.geo, app_limits(cache_bytes))
            .await;
        let ep = crate::api::routes(app.clone());
        let path = "/collections/buildings/items?sources=2&limit=2";

        let (status, first) = get(&ep, path).await;
        assert_eq!(status, 200, "{first}");
        let (status, second) = get(&ep, path).await;
        assert_eq!(status, 200, "{second}");
        assert_eq!(first, second);

        // Conditional request against the cached entry: 304 with the same ETag.
        let etag = etag_of(&ep, path).await;
        let response = ep
            .get_response(
                Request::builder()
                    .uri(path.parse().unwrap())
                    .header("if-none-match", &etag)
                    .finish(),
            )
            .await;
        assert_eq!(response.status().as_u16(), 304);

        // A different effective source set must not collide with the entry.
        let (status, other) = get(&ep, "/collections/buildings/items?sources=3&limit=2").await;
        assert_eq!(status, 200, "{other}");
        assert_ne!(first, other);

        // Same effective selection via query+header intersection shares one entry.
        let response = ep
            .get_response(
                Request::builder()
                    .uri(
                        "/collections/buildings/items?sources=2,3&limit=2"
                            .parse()
                            .unwrap(),
                    )
                    .header("x-source-ids", "2")
                    .finish(),
            )
            .await;
        assert_eq!(response.status().as_u16(), 200);

        // Tiles repeat through the cache too (204 when the fixture tile is empty).
        let first_tile = get(&ep, "/collections/buildings/tiles/12/2103/1346?sources=2")
            .await
            .0;
        let second_tile = get(&ep, "/collections/buildings/tiles/12/2103/1346?sources=2")
            .await
            .0;
        assert_eq!(first_tile, second_tile);

        // Validation errors are never cached: repeats still re-validate.
        for _ in 0..2 {
            assert_eq!(
                get(&ep, "/collections/buildings/items?bbox=2,0,1,1")
                    .await
                    .0,
                400
            );
        }

        let exposition = app.metrics.exposition();
        if cache_bytes > 0 {
            // items x2 -> 1 hit, 304 -> 1 hit, header-intersection -> 1 hit,
            // tile -> 1 hit; the different-source and error requests miss.
            assert!(
                exposition.contains("lakewing_response_cache_hits_total 4\n"),
                "{exposition}"
            );
        } else {
            assert!(exposition.contains("lakewing_response_cache_hits_total 0\n"));
        }
        assert!(exposition.contains("lakewing_in_flight 0\n"));
    }
}

fn app_limits(response_cache_bytes: usize) -> crate::app::Limits {
    crate::app::Limits {
        concurrency: 2,
        response_cache_bytes,
        ..Default::default()
    }
}

async fn etag_of(ep: &impl Endpoint, path: &str) -> String {
    let response = ep
        .get_response(Request::builder().uri(path.parse().unwrap()).finish())
        .await;
    response
        .headers()
        .get("etag")
        .expect("etag")
        .to_str()
        .expect("ascii")
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_geometry_schema_admission_and_nonspatial() {
    let fixture = Fixture::new().await;
    for uri in [&fixture.wkb, &fixture.geo, &fixture.plain] {
        let app = fixture.app(uri, 1).await;
        let server = FlightServer::new(app.clone());
        let ep = crate::api::routes(app.clone());
        let ticket = |json: Value| {
            tonic::Request::new(Ticket {
                ticket: serde_json::to_vec(&json).unwrap().into(),
            })
        };
        let params = serde_json::json!({"collection":"buildings", "sources":[2], "columns":["id","geometry","source_id","score"], "limit":10});
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(&params).unwrap());
        let advertised = server
            .get_schema(tonic::Request::new(descriptor))
            .await
            .unwrap()
            .into_inner();
        let expected_schema = arrow::datatypes::Schema::try_from(&advertised).unwrap();
        let response = server.do_get(ticket(params.clone())).await.unwrap();
        assert_eq!(get(&ep, "/collections/buildings/items").await.0, 429);
        let mut stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        let mut found = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let geometry = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                found.push(ids.value(i).to_string());
                if uri == &fixture.plain || ids.value(i) == "d" {
                    assert!(geometry.is_null(i));
                } else {
                    let original_ids = fixture
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let sources = fixture
                        .batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let original = fixture
                        .batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    let row = (0..original.len()).find(|j| original_ids.value(*j) == ids.value(i) && sources.value(*j) == 2).unwrap_or_else(|| panic!("unexpected Flight ID {:?}; originals={original_ids:?}, sources={sources:?}", ids.value(i)));
                    assert_eq!(geometry.value(i), original.value(row));
                }
            }
        }
        assert_eq!(found, ["a", "b", "c", "d"]);
        drop(stream);
        assert!(app.metrics.exposition().contains("lakewing_in_flight 0\n"));

        // Flight bbox shares the exact Lance filter: the polygon-hole 'a' must not stream.
        let mut bounded = params.clone();
        bounded["bbox"] = serde_json::json!([4.0, 4.0, 6.0, 6.0]);
        let response = server.do_get(ticket(bounded)).await;
        if uri == &fixture.plain {
            assert_eq!(response.err().unwrap().code(), Code::InvalidArgument);
        } else {
            let mut bounded = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                response
                    .unwrap()
                    .into_inner()
                    .map_err(arrow_flight::error::FlightError::from),
            );
            let mut ids = Vec::new();
            while let Some(batch) = bounded.try_next().await.unwrap() {
                for i in 0..batch.num_rows() {
                    ids.push(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(i)
                            .to_string(),
                    );
                }
            }
            assert_eq!(ids, ["b", "c"]);
            drop(bounded);
        }

        let mut empty = params.clone();
        empty["sources"] = serde_json::json!([]);
        let response = server.do_get(ticket(empty)).await.unwrap();
        let mut empty = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        assert!(empty.try_next().await.unwrap().is_none());
        assert_eq!(empty.schema().unwrap().as_ref(), &expected_schema);
        drop(empty);

        let permit = app.admit(false).unwrap();
        assert_eq!(
            server
                .do_get(ticket(params.clone()))
                .await
                .err()
                .unwrap()
                .code(),
            Code::ResourceExhausted
        );
        drop(permit);
        let response = server.do_get(ticket(params)).await.unwrap();
        drop(response); // A disconnected consumer releases admission.
        assert!(app.admit(false).is_ok());
        let (status, body) = get(&ep, "/collections/buildings/items?sources=2&bbox=4,4,6,6").await;
        assert_eq!(
            status,
            if uri == &fixture.plain { 400 } else { 200 },
            "{body}"
        );
        if uri == &fixture.plain {
            assert_eq!(
                get(&ep, "/collections/buildings/items/b?sources=2").await.1["geometry"],
                Value::Null
            );
        }
    }
}
