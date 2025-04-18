# Create Postgres Database Storage
mkdir postgres-data
chmod 777 postgres-data
# Connecting to the database:

## Connect to the PostgreSQL container
docker exec -it iceberg_rest_catalog-postgres-1 bash

## Inside the container, connect to PostgreSQL
psql -U iceberg -d iceberg_catalog

# PREFIX Logic

## Prefix Middleware

The prefix middleware is used to rewrite the path to the API format.
Now your API will be able to handle requests from Spark when configured like:
.config("spark.sql.catalog.rest_catalog.uri", "http://100.28.115.226:8000/prod")
The client will make requests to:

http://100.28.115.226:8000/prod/v1/config
http://100.28.115.226:8000/prod/v1/namespaces

The middleware will rewrite these to:

http://100.28.115.226:8000/v1/config?warehouse=prod
http://100.28.115.226:8000/v1/prod/namespaces

Your existing route handlers will continue to work without modification because the rewriting happens at the middleware level before the request reaches your route handlers.
This approach combines the best of both worlds:

Clients can use the intuitive tenant prefix in their catalog URI
Your API maintains its clean route structure with the prefix as a path parameter
No changes are needed to the existing endpoints or services

Your API is now ready to handle both direct calls with the prefix in the path parameter and client calls with the prefix at the beginning of the URL.

Now that the config and namespace endpoints are working, lets move on to the table endpoint. Please follow the Iceberg_Rest_catalog_Requirement.md, pydantic_model.py files in the project knowledge and the OpenAPI spec document attached to this chat conversation. Please review all the documents before starting to code. The database models are already defined, so please use that. Please make sure to follow the design document for all the namespace related endpoints. Please add a lot of logging where ever possible. Remember how prefix is used and handled within the code. lets start coding the table endpoints.

In # app/services/table.py under load_credentials() function, did you complete the code to fetch credentials from the database based on the table's storage location?
Can we complete this code please.

In # app/services/table.py did you complete the async def report_metrics() function??

Missing API Endpoints and Service Functions
For the missing endpoints and functions, I'll outline what needs to be implemented:
1. POST /v1/{prefix}/namespaces/{namespace}/tables/{table} - Commit updates to a table
This endpoint needs to be added to tables.py with the corresponding commit_table function in table.py. The function should validate the provided requirements against the current table state and apply the updates if the requirements are met.
2. POST /v1/{prefix}/namespaces/{namespace}/register - Register a table
This endpoint needs to be added to tables.py with the corresponding register_table function in table.py. The function should register a table using a provided metadata file location.
3. Table scan planning endpoints
The following scan planning endpoints need to be implemented:

POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan
GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}
DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks

These require corresponding service functions:

plan_table_scan
fetch_planning_result
cancel_planning
fetch_scan_tasks

4. POST /v1/{prefix}/transactions/commit - Commit transaction
This endpoint needs to be added along with a commit_transaction function that can apply updates to multiple tables in an atomic operation.
Conclusion
To fully implement the Iceberg REST Catalog API according to the OpenAPI spec and requirements document, you need to:

Complete the report_metrics() and load_credentials() functions as shown above
Implement the missing endpoints and service functions for:

Committing updates to a table
Registering a table
Table scan planning operations
Committing transactions

-----
For Testing
For testing, you can insert global credentials that apply to all your tables:
sqlINSERT INTO storage_credentials (prefix, config, table_id) VALUES 
('s3://300289082521-my-warehouse/', 
 '{"region": "us-east-1", "access-key-id": "YOUR_ACCESS_KEY", "secret-access-key": "YOUR_SECRET_KEY"}',
 NULL);

 For Production
For production, you have several better options:

Instance Profile Credentials:
sqlINSERT INTO storage_credentials (prefix, config, table_id) VALUES 
('s3://300289082521-my-warehouse/', 
 '{"region": "us-east-1", "use-instance-credentials": "true"}',
 NULL);

Temporary Credentials via STS:
sqlINSERT INTO storage_credentials (prefix, config, table_id) VALUES 
('s3://300289082521-my-warehouse/', 
 '{"region": "us-east-1", "sts-role-arn": "arn:aws:iam::account:role/role-name", "duration-seconds": "3600"}',
 NULL);

 Usage Examples
1. Adding Global Credentials via API
bashcurl -X 'POST' \
  'http://0.0.0.0:8000/v1/dev/credentials' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "prefix": "dev",
    "warehouse": "s3://300289082521-my-warehouse/dev/",
    "config": {
      "region": "us-east-1",
      "use-instance-credentials": "true"
    }
  }'
2. Creating a Table with Credentials

curl -X 'POST' \
  'http://0.0.0.0:8000/v1/dev/namespaces/namespace1/tables' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "customer_data1",
    "location": "s3://300289082521-my-warehouse/dev/customer_data1",
    "schema": {
      "type": "struct",
      "fields": [
        { "id": 1, "name": "id", "type": "long", "required": true },
        { "id": 2, "name": "name", "type": "string", "required": true }
      ]
    },
    "credentials": {
      "config": {
        "region": "us-east-1",
        "use-instance-credentials": "true"
      }
    }
  }'