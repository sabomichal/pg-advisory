-- Lock table of pg-advisory. Optional: the library can create it itself (pg-advisory.schema.auto-create=true).
CREATE TABLE IF NOT EXISTS advisory_lock
(
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- advisory lock id of the key
    lock_key text   NOT NULL UNIQUE
);
