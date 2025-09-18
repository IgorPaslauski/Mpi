CREATE TABLE IF NOT EXISTS numbers (
    id bigserial PRIMARY KEY,
    slave_id int NOT NULL,
    value int NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_numbers_slave ON numbers (slave_id);