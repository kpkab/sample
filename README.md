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