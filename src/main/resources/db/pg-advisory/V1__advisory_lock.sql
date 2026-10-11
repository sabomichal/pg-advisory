-- Lock table of pg-advisory. Optional: the library can create it itself (spring.pg-advisory.schema.auto-create=true).
CREATE TABLE IF NOT EXISTS advisory_lock
(
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- advisory lock id (KeyMapper.Registry)
    lock_key   text   NOT NULL UNIQUE,
    generation bigint NOT NULL DEFAULT 0,                       -- fencing token, incremented on every acquisition
    owner      text                                             -- owner of the last acquisition
);
