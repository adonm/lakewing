-- Layout v3 (db::LAYOUT_VERSION); gateways reject incompatible layouts.
--
-- Object bytes are fixed-size INLINE rows: 8120-byte payloads stay inline
-- with toast_tuple_target = 8160 (heaptoast.c only externalises while
-- data_size > RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL
-- columns, so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB
-- page, 99.6% fill, no TOAST, no toast-pointer indirection; slices are memcpy.
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.layout (version int4 NOT NULL);

CREATE TABLE IF NOT EXISTS s3p.buckets (
  name       text COLLATE "C" PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- bucket/key collate "C": S3 lists keys in byte order, so the primary key
-- serves both point lookups and ordered LIST range scans.
CREATE TABLE IF NOT EXISTS s3p.objects (
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  file_id    bigserial        NOT NULL UNIQUE,  -- the single file, or the first part
  size       int8             NOT NULL,
  etag       bytea            NOT NULL,         -- sha256 of the bytes; multipart: of the part sha256s
  created_at timestamptz      NOT NULL DEFAULT now(),
  parts      int8[],                            -- multipart: part file_ids in order
  part_ends  int8[],                            -- multipart: cumulative end offsets
  PRIMARY KEY (bucket, key),
  FOREIGN KEY (bucket) REFERENCES s3p.buckets (name)
) WITH (autovacuum_vacuum_scale_factor = 0.02);

-- Hash-partitioned by file_id, 32 ways. One relation caps at MaxBlockNumber
-- (0xFFFFFFFE) x 8 KB = 32 TiB (storage/block.h): ~31.7 TiB of object data at
-- one row per page. Concurrent writers (one per PUT / multipart part, with
-- adjacent file_ids) land on 32 heaps and 32 primary-key right edges instead
-- of contending on one (LWLock:BufferContent / Lock:Extend in Performance
-- Insights); every GET is `file_id = $1` and prunes to exactly one partition.
-- Partitioned parents take no storage parameters (reloptions.c), so
-- toast_tuple_target is set per partition; STORAGE EXTERNAL is inherited
-- (tablecmds.c MergeAttributes). Partitioned tables cannot be UNLOGGED.
CREATE TABLE IF NOT EXISTS s3p.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea STORAGE EXTERNAL NOT NULL,
  PRIMARY KEY (file_id, no)
) PARTITION BY HASH (file_id);

DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS s3p.chunks_%s PARTITION OF s3p.chunks '
      'FOR VALUES WITH (MODULUS 32, REMAINDER %s) '
      'WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01, '
      'autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)',
      lpad(i::text, 2, '0'), i);
  END LOOP;
END $$;

-- In-progress multipart uploads live in PostgreSQL, not gateway memory: any
-- gateway can take any part and uploads survive gateway restarts.
CREATE TABLE IF NOT EXISTS s3p.uploads (
  upload_id  text             PRIMARY KEY,
  bucket     text COLLATE "C" NOT NULL,
  key        text COLLATE "C" NOT NULL,
  created_at timestamptz      NOT NULL DEFAULT now(),
  UNIQUE (bucket, key),
  FOREIGN KEY (bucket) REFERENCES s3p.buckets (name)
);

CREATE TABLE IF NOT EXISTS s3p.upload_parts (
  upload_id text  NOT NULL REFERENCES s3p.uploads ON DELETE CASCADE,
  part_no   int4  NOT NULL,
  file_id   int8  NOT NULL,
  size      int8  NOT NULL,
  sha256    bytea NOT NULL,
  PRIMARY KEY (upload_id, part_no)
);
CREATE INDEX IF NOT EXISTS uploads_by_age ON s3p.uploads (created_at, upload_id);

-- Each cron invocation deletes at most p_max_rows chunks and visits at most
-- 64 stale uploads/parts, including empty uploads that have no chunk budget.
CREATE OR REPLACE FUNCTION s3p.expire_uploads(
  p_grace interval DEFAULT interval '24 hours',
  p_max_rows int DEFAULT 4096,
  p_bucket text DEFAULT NULL
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  victim_upload text;
  victim_part int;
  victim_file bigint;
  removed int := 0;
  batch_removed int;
  remaining int;
BEGIN
  IF p_grace IS NULL OR p_max_rows IS NULL OR
     p_grace < interval '0 seconds' OR p_max_rows < 1 OR p_max_rows > 8192 THEN
    RAISE EXCEPTION 'invalid upload expiry budget';
  END IF;
  FOR attempt IN 1..64 LOOP
    SELECT upload_id INTO victim_upload FROM s3p.uploads
      WHERE created_at < now() - p_grace AND (p_bucket IS NULL OR bucket = p_bucket)
      ORDER BY created_at, upload_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN victim_upload IS NULL;
    SELECT part_no, file_id INTO victim_part, victim_file FROM s3p.upload_parts
      WHERE upload_id = victim_upload ORDER BY part_no LIMIT 1;
    IF victim_part IS NULL THEN
      DELETE FROM s3p.uploads WHERE upload_id = victim_upload;
      CONTINUE;
    END IF;
    remaining := p_max_rows - removed;
    WITH doomed AS (
      SELECT no FROM s3p.chunks WHERE file_id = victim_file
        ORDER BY no LIMIT remaining
    )
    DELETE FROM s3p.chunks c USING doomed d
      WHERE c.file_id = victim_file AND c.no = d.no;
    GET DIAGNOSTICS batch_removed = ROW_COUNT;
    removed := removed + batch_removed;
    IF batch_removed < remaining THEN
      DELETE FROM s3p.upload_parts
        WHERE upload_id = victim_upload AND part_no = victim_part;
      IF NOT EXISTS (SELECT 1 FROM s3p.upload_parts WHERE upload_id = victim_upload) THEN
        DELETE FROM s3p.uploads WHERE upload_id = victim_upload;
      END IF;
    END IF;
    EXIT WHEN removed = p_max_rows;
  END LOOP;
  RETURN removed;
END $$;
