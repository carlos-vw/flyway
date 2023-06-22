CREATE TABLE IF NOT EXISTS flyway_demo.person (
    id INT,
    name STRING
)
LOCATION 's3://flyway-demo/'
TBLPROPERTIES ('table_type'='iceberg','format'='parquet');