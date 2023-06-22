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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class AthenaSchema extends Schema {

    public AthenaSchema(JdbcTemplate jdbcTemplate, Database database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=?",
                name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !jdbcTemplate.queryForBoolean("SELECT EXISTS (\n" +
                "    SELECT table_name FROM information_schema.tables t\n" +
                "    WHERE  t.table_schema = ?\n" +
                ")", name);
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.executeStatement("CREATE SCHEMA " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.executeStatement("DROP SCHEMA " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {
        for (String statement : generateDropStatementsForViews()) {
            jdbcTemplate.executeStatement(statement);
        }

        for (Table table : allTables()) {
            table.drop();
        }
    }

    private List<String> generateDropStatementsForViews()
            throws SQLException {
        List<String> viewNames = jdbcTemplate.queryForStringList(
                "SELECT table_name FROM information_schema.tables" +
                        " WHERE table_type = 'VIEW' AND table_schema = ?",
                name);
        List<String> statements = new ArrayList<>();
        for (String domainName : viewNames) {
            statements.add("DROP VIEW IF EXISTS " + database.quote(name, domainName));
        }

        return statements;
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "SELECT table_name FROM information_schema.tables" +
                        " WHERE table_type = 'BASE TABLE' AND table_schema = ?",
                name);
        AthenaTable[] tables = new AthenaTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new AthenaTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new AthenaTable(jdbcTemplate, database, this, tableName);
    }

}
