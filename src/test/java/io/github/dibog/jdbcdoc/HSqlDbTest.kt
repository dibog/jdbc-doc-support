package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.jdbc.core.JdbcTemplate

const val HSQL_SCHEMA = "test"

@TestInstance(PER_CLASS)
class HSqlDbTest : BaseTestDB("public", HSQL_SCHEMA, createJdbcTemplate(HSQL_SCHEMA)) {

    companion object {
        fun createJdbcTemplate(schema: String) = JdbcTemplate(setupDataSource()).apply {
            this.execute("drop schema if exists $schema cascade;")
            this.execute("create schema $schema;")
        }
    }

    @AfterAll
    fun shutdown() {
//        jdbc.execute("drop schema if exists $schema cascade;")
        jdbc.execute("SHUTDOWN")
    }
}
