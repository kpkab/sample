CREATE TABLE IF NOT EXISTS catalog_config (
    id SERIAL PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL UNIQUE,
    config_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert default configuration (make sure the JSON is properly formatted)
INSERT INTO catalog_config (catalog_name, config_json) 
VALUES ('default', 
    '{"overrides": {"warehouse": "s3://300289082521-my-warehouse/dev/"}, "defaults": {"clients": "4"}, "endpoints": ["GET /v1/{prefix}/namespaces/{namespace}", "GET /v1/{prefix}/namespaces", "POST /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}", "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"]}'::jsonb
)
ON CONFLICT (catalog_name) DO NOTHING;

-- docker/postgres/init.sql (add to existing file)

-- Namespaces table
CREATE TABLE IF NOT EXISTS namespaces (
    id SERIAL PRIMARY KEY,
    levels TEXT[] NOT NULL,
    properties JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (levels)
);


-- Views reference (needed for DELETE check)
CREATE TABLE IF NOT EXISTS views (
    id SERIAL PRIMARY KEY,
    namespace_id INTEGER REFERENCES namespaces(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    -- Other fields would be here
    UNIQUE (namespace_id, name)
);

-- docker/postgres/init.sql (additional table-related schema)

-- Tables 
CREATE TABLE IF NOT EXISTS tables (
    id SERIAL PRIMARY KEY,
    namespace_id INTEGER NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    table_uuid UUID NOT NULL,
    location TEXT NOT NULL,
    current_snapshot_id BIGINT,
    last_sequence_number BIGINT DEFAULT 0,
    last_updated_ms BIGINT NOT NULL,
    last_column_id INTEGER NOT NULL,
    schema_id INTEGER NOT NULL,
    current_schema_id INTEGER NOT NULL,
    default_spec_id INTEGER NOT NULL,
    last_partition_id INTEGER NOT NULL,
    default_sort_order_id INTEGER NOT NULL,
    properties JSONB DEFAULT '{}'::jsonb,
    format_version INTEGER NOT NULL,
    row_lineage BOOLEAN DEFAULT FALSE,
    next_row_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (namespace_id, name)
);

-- Schemas
CREATE TABLE IF NOT EXISTS schemas (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    schema_id INTEGER NOT NULL,
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, schema_id)
);

-- Partition specs
CREATE TABLE IF NOT EXISTS partition_specs (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    spec_id INTEGER NOT NULL,
    spec_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, spec_id)
);

-- Sort orders
CREATE TABLE IF NOT EXISTS sort_orders (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    order_id INTEGER NOT NULL,
    order_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, order_id)
);

-- Snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id SERIAL PRIMARY KEY, 
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    snapshot_id BIGINT NOT NULL,
    parent_snapshot_id BIGINT,
    sequence_number BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    manifest_list TEXT NOT NULL,
    summary JSONB NOT NULL,
    schema_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, snapshot_id)
);

-- Snapshot references (branches/tags)
CREATE TABLE IF NOT EXISTS snapshot_refs (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    snapshot_id BIGINT NOT NULL,
    type VARCHAR(50) NOT NULL,
    min_snapshots_to_keep INTEGER,
    max_snapshot_age_ms BIGINT,
    max_ref_age_ms BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, name)
);

-- Table Statistics
CREATE TABLE IF NOT EXISTS table_statistics (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    snapshot_id BIGINT NOT NULL,
    statistics_path TEXT NOT NULL,
    file_size_in_bytes BIGINT NOT NULL,
    file_footer_size_in_bytes BIGINT NOT NULL,
    blob_metadata JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, snapshot_id)
);

-- Partition Statistics
CREATE TABLE IF NOT EXISTS partition_statistics (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    snapshot_id BIGINT NOT NULL,
    statistics_path TEXT NOT NULL,
    file_size_in_bytes BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (table_id, snapshot_id)
);

-- Storage credentials
CREATE TABLE storage_credentials (
    id SERIAL PRIMARY KEY,
    prefix TEXT NOT NULL,  -- dev, test, hr, marketing, sales
    warehouse TEXT NOT NULL,  -- s3://300289082521-my-warehouse/dev/, etc.
    config JSONB NOT NULL,
    table_id INTEGER REFERENCES tables(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (prefix, warehouse, table_id)
);

-- Operation metrics
CREATE TABLE IF NOT EXISTS operation_metrics (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    report_type VARCHAR(50) NOT NULL,
    snapshot_id BIGINT,
    sequence_number BIGINT,
    operation VARCHAR(50),
    filter_json JSONB,
    schema_id INTEGER,
    projected_field_ids INTEGER[],
    projected_field_names TEXT[],
    metrics_json JSONB NOT NULL,
    metadata_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Metadata log for tables
CREATE TABLE IF NOT EXISTS metadata_log (
    id SERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
    metadata_file TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);