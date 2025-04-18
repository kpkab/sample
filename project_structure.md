iceberg_rest_catalog/
├── app/
│   ├── __init__.py
│   ├── main.py                     # Main FastAPI application
│   ├── database.py                 # Database connection handling
│   ├── models/                     # Models directory
│   │   ├── __init__.py
│   │   ├── base.py                 # Base models (errors, common types)
│   │   ├── config.py               # Config models
│   │   ├── namespace.py            # Namespace models
│   │   ├── table.py                # Table models
│   │   └── view.py                 # View models
│   ├── api/                        # API routes
│   │   ├── __init__.py
│   │   ├── config.py               # Config endpoint router
│   │   ├── namespaces.py           # Namespace endpoints
│   │   ├── tables.py               # Table endpoints
│   │   └── views.py                # View endpoints
│   ├── services/                   # Service layer
│   │   ├── __init__.py
│   │   ├── config.py               # Config service implementation
│   │   ├── namespace.py            # Namespace service implementation
│   │   ├── table.py                # Table service implementation 
│   │   └── view.py                 # View service implementation
│   └── utils/                      # Utility functions
│       ├── __init__.py
│       └── error_handlers.py       # Common error handling functions
├── docker/                         # Docker related files
│   ├── app/                        # FastAPI Dockerfile and configs
│   │   └── Dockerfile
│   └── postgres/                   # PostgreSQL Dockerfile and configs
│       ├── Dockerfile
│       └── init.sql                # Initial database setup
├── docker-compose.yml              # Docker Compose configuration
├── requirements.txt
└── README.md