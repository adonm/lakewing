-- pgvs3 storage layout v2 on PostgreSQL 18 (db::LAYOUT_VERSION). A gateway
-- refuses to serve a layout it was not built for (s3p.layout); breaking
-- layout changes bump the version, at most once per major release.
--
-- Object bytes are fixed-size INLINE rows: 8120-byte payloads stay inline
-- with toast_tuple_target = 8160 (heaptoast.c only externalises while
-- data_size > RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL
-- columns, so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB
-- page, 99.6% fill, no TOAST, no toast-pointer indirection; slices are memcpy.
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.layout (version int4 NOT NULL);

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
  PRIMARY KEY (bucket, key)
);
-- file_id -> owning multipart object, for the janitor's orphan sweep.
CREATE INDEX IF NOT EXISTS objects_parts ON s3p.objects USING gin (parts);

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
      'FOR VALUES WITH (MODULUS 32, REMAINDER %s) WITH (toast_tuple_target = 8160)',
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
  UNIQUE (bucket, key)
);

CREATE TABLE IF NOT EXISTS s3p.upload_parts (
  upload_id text  NOT NULL REFERENCES s3p.uploads ON DELETE CASCADE,
  part_no   int4  NOT NULL,
  file_id   int8  NOT NULL,
  size      int8  NOT NULL,
  sha256    bytea NOT NULL,
  PRIMARY KEY (upload_id, part_no)
);
CREATE INDEX IF NOT EXISTS upload_parts_file ON s3p.upload_parts (file_id);
