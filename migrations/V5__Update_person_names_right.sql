-- https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html
UPDATE flyway_demo.person SET first_name='Foo' WHERE first_name='Mr. Foo';
UPDATE flyway_demo.person SET first_name='Bar' WHERE first_name='Ms. Bar';
