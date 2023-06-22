CREATE EXTERNAL TABLE flyway_demo.person (
    id int,
    name varchar(100)
)
LOCATION 's3://flyway-demo/';