-- pgvs3: S3 objects as fixed-size INLINE byte rows on PostgreSQL 18.
--
-- 8120-byte payloads stay fully inline with toast_tuple_target = 8160
-- (heaptoast.c only externalises while data_size >
--  RelationGetToastTupleTarget - hoff; hoff = 24 with all-NOT-NULL columns,
--  so 8 + 4 + 4 + 8120 = 8136 <= 8136): one 8160-byte tuple per 8 KB page,
--  99.6% fill, no TOAST relation, no toast-pointer indirection. Slices are
--  memcpy (detoast_attr_slice only branches for on-disk externals).
CREATE SCHEMA IF NOT EXISTS s3p;

CREATE TABLE IF NOT EXISTS s3p.objects (
  bucket     text        NOT NULL,
  key        text        NOT NULL,
  file_id    bigserial   NOT NULL UNIQUE,
  size       int8        NOT NULL,
  etag       bytea       NOT NULL,   -- sha256 of the object bytes
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, key)
);

-- S3 lists keys in UTF-8 byte order; keep range scans in the same order.
CREATE INDEX IF NOT EXISTS objects_key_c ON s3p.objects (bucket, key COLLATE "C");

CREATE TABLE IF NOT EXISTS s3p.chunks (
  file_id int8  NOT NULL,
  no      int4  NOT NULL,
  data    bytea STORAGE EXTERNAL NOT NULL,
  PRIMARY KEY (file_id, no)
) WITH (toast_tuple_target = 8160);

-- Multipart objects are the ordered list of their part files: each part
-- ingests as its own file_id the moment it arrives and Complete only
-- publishes. NULL = one file (file_id holds all `size` bytes). Checked first
-- so gateways (which run this at start) take no table lock once present.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 's3p' AND table_name = 'objects' AND column_name = 'parts') THEN
    ALTER TABLE s3p.objects ADD COLUMN parts int8[], ADD COLUMN part_ends int8[];
  END IF;
END $$;

-- In-progress multipart uploads live in Aurora, not gateway memory: any
-- gateway can take any part and uploads survive gateway restarts.
CREATE TABLE IF NOT EXISTS s3p.uploads (
  upload_id  text        PRIMARY KEY,
  bucket     text        NOT NULL,
  key        text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
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
