CREATE TABLE IF NOT EXISTS fraud_scores (
    id BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    transaction_id VARCHAR(128) PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0,1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS scores_latest ON fraud_scores (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS fraud_latest ON fraud_scores (created_at DESC, id DESC) WHERE fraud_flag = 1;
