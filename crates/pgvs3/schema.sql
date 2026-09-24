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
