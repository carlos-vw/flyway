-- https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-evolving-table-schema.html
ALTER TABLE flyway_demo.person CHANGE name first_name string AFTER id