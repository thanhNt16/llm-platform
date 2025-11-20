import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa

# Create a sample DataFrame
df = pl.DataFrame(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.0, 30.75]}
)

# Convert Polars DataFrame to PyArrow Table to extract schema
arrow_table = df.to_arrow()
arrow_schema = arrow_table.schema

# Configure SQL catalog with MinIO (S3-compatible) settings
# Use a local SQLite for the catalog metadata, MinIO for data storage
catalog = SqlCatalog(
    "local",
    **{
        "uri": "sqlite:///:memory:",  # In-memory for testing; use a file path for persistence, e.g., "sqlite:///catalog.db"
        "warehouse": "s3://local-lakehouse/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",  # Replace with your access key
        "s3.secret-access-key": "password",  # Replace with your secret key
        "s3.path-style-access": "true",  # Needed for MinIO
        "s3.region": "us-east-1",
    },
)

# Create the default namespace if it doesn't exist
namespace = "default"
try:
    catalog.create_namespace(namespace)
    print(f"Created namespace '{namespace}'.")
except Exception:
    print(f"Namespace '{namespace}' already exists.")

# Create the Iceberg table if it doesn't exist
table_identifier = f"{namespace}.test_table"
try:
    table = catalog.load_table(table_identifier)
    print(f"Table '{table_identifier}' already exists; will append/overwrite.")
except Exception:
    table = catalog.create_table(table_identifier, schema=arrow_schema)
    print(f"Created new Iceberg table '{table_identifier}'.")

# Write the DataFrame to the Iceberg table
df.write_iceberg(
    target=table,
    mode="append",  # Or "overwrite" to replace existing data
)

print(
    f"DataFrame written to Iceberg table '{table_identifier}' in s3://local-lakehouse/."
)
