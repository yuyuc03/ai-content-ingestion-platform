-- 记录系统里有哪一些数据源、每个数据源是什么类型
CREATE TABLE IF NOT EXISTS source_registry (
    source_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 每一次接入脚本跑一次都会在这边记录
-- 记录从哪一种数据源接入的、什么时候接入的、跑了多久、成功与否、抓了多少条数据
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES source_registry(source_id),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status VARCHAR(20),
    records_ingested INTEGER DEFAULT 0,
    records_filed INTEGER DEFAULT 0
);

-- 清理后的数据最终会存在这里
CREATE TABLE IF NOT EXISTS content_items (
    item_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES source_registry(source_id),
    run_id INTEGER REFERENCES ingestion_runs(run_id),
    external_id VARCHAR(255) NOT NULL,
    title TEXT,
    author VARCHAR(255),
    published_at TIMESTAMP,
    content_type VARCHAR(50),
    url TEXT,
    view_count BIGINT,
    like_count BIGINT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(source_id, external_id)
);

-- 校验失败的记录存在这里，而不是直接丢弃
CREATE TABLE IF NOT EXISTS data_quality_logs (
    log_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES ingestion_runs(run_id),
    source VARCHAR(50),
    rule_name VARCHAR(100),
    message TEXT,
    raw_data JSONB,
    logged_at TIMESTAMP DEFAULT NOW()
);

-- 预先插入三种数据源
INSERT INTO source_registry (name, type) VALUES
    ('YouTube Data', 'api'),
    ('RSS Feeds', 'rss'),
    ('Batch Files', 'batch')
ON CONFLICT DO NOTHING;