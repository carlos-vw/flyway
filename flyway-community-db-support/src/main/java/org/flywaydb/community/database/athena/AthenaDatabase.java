/*
 * Copyright (C) Red Gate Software Ltd 2010-2023
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.community.database.athena;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

public class AthenaDatabase extends Database {

    public AthenaDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory,
            StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected Connection doGetConnection(java.sql.Connection connection) {
        return new AthenaConnection(this, connection);
    }

    @Override
    public void ensureSupported() {
        return;
    }

    @Override
    public boolean supportsDdlTransactions() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "TRUE";
    }

    @Override
    public String getBooleanFalse() {
        return "FALSE";
    }

    @Override
    public boolean catalogIsSchema() {
        // Athena:
        // data source is catalog -> AwsDataCatalog
        // database is schema -> flyway
        // Reference: https://docs.aws.amazon.com/athena/latest/ug/understanding-tables-databases-and-the-data-catalog.html
        return false;
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        // References:
        // https://docs.aws.amazon.com/athena/latest/ug/create-table.html
        // https://aws.amazon.com/blogs/big-data/amazon-emr-supports-apache-hive-acid-transactions/ 
        return "CREATE EXTERNAL TABLE " + table.getSchema().getName() + "." + table.getName() + " (\n" +
                "    installed_rank INT,\n" +
                "    version VARCHAR(50),\n" +
                "    description VARCHAR(200),\n" +
                "    type VARCHAR(20),\n" +
                "    script VARCHAR(1000),\n" +
                "    checksum INTEGER,\n" +
                "    installed_by VARCHAR(100),\n" +
                "    installed_on TIMESTAMP,\n" +
                "    execution_time INTEGER,\n" +
                "    success BOOLEAN\n" +
                ")\n" +
                "LOCATION 's3://flyway-gpsa-development/'\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }
    
}
