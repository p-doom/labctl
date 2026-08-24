ALTER TABLE artifacts
    ADD COLUMN content_hash TEXT,
    ADD CONSTRAINT artifacts_content_hash_format
        CHECK (content_hash ~ '^[0-9a-f]{64}$');

