package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.jdbc.core.JdbcTemplate

const val PSQL_SCHEMA = "test"

@TestInstance(PER_CLASS) @Disabled
class PostgresTest : BaseTestDB("isr", PSQL_SCHEMA, createJdbcTemplate(PSQL_SCHEMA)) {

    companion object {
        fun createJdbcTemplate(schema: String) = JdbcTemplate(setupIsrDocTestDataSource()).apply {
            this.execute("drop schema if exists $schema cascade;")
            this.execute("create schema $schema;")
        }
    }

    @AfterAll
    fun shutdown() {
//        jdbc.execute("drop schema if exists $schema CASCADE;")
    }
}
