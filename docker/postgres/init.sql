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
