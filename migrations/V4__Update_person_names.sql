-- https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html
UPDATE flyway_demo.person SET first_name='Mr. Foo' WHERE first_name='Foo';
UPDATE flyway_demo.person SET first_name='Ms. Bar' WHERE first_name='Bar';
