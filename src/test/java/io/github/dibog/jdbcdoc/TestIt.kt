package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.jdbc.core.JdbcTemplate

@TestInstance(PER_CLASS)
class TestIt {
    private val jdbc = JdbcTemplate(setupDataSource()).apply {
        createDatabase()
        initializeDatabase()
    }

    private fun JdbcTemplate.initializeDatabase() {

        execute("""
        create table test.foo1 (
            id int not null,
            name varchar(20) not null,
            CONSTRAINT PK_FOO1 PRIMARY KEY (id),
            CONSTRAINT UC_FOO1_NAME UNIQUE (name)
        );
        """.trimIndent())

        execute("""
        create table test.foo2 (
            id int not null,
            foo_id int not null,
            my_check varchar(20),
            CONSTRAINT FK_FOO2_ID FOREIGN KEY (id) REFERENCES test.foo1(id),
            CONSTRAINT PK_FOO2 PRIMARY KEY (id),
            CONSTRAINT CH_CHECK CHECK (my_check!='foo' AND id<20)
        );
        """.trimIndent())

        execute("""
        create table test.foo3 (
            id1 int not null,
            id2 int not null,
            CONSTRAINT PK_FOO3 PRIMARY KEY (id1, id2),
            CONSTRAINT FK_FOO3_ID1 FOREIGN KEY (id1) REFERENCES test.foo1(id),
            CONSTRAINT FK_FOO3_ID2 FOREIGN KEY (id2) REFERENCES test.foo2(id),
        );
        """.trimIndent())
    }

    @AfterAll
    fun shutdown() {
        jdbc.execute("SHUTDOWN")
    }

    @Test
    fun collectCheckConstraints() {
        println("my_own_check_constraint_query")
        jdbc.fetchAllCheckConstraintsOf("public", "test").toTableString(
                headers = listOf("Full Column Name", "Full Constraint Name", "Check Clause")
        ) { (colName, consName, clause) ->
            listOf(colName.toString(), consName.toString(), clause)
        }.println()
    }

    @Test
    fun collectPrimaryKeys() {
        println("my_own_primary_key_query")
        jdbc.fetchAllPrimaryKeyConstraintsOf("public", "test").toTableString(
                headers = listOf("Full Column Name", "Full Constraint Name")
        ) { (colName, consName) ->
            listOf(colName.toString(), consName.toString())
        }.println()
    }

    @Test
    fun collectUniqueConstraints() {
        println("my_own_unique_query")
        jdbc.fetchAllUniqueConstraintsOf("public", "test").toTableString(
                headers = listOf("Full Column Name", "Full Constraint Name")
        ) { (colName, consName) ->
            listOf(colName.toString(), consName.toString())
        }.println()
    }

    @Test
    fun collectForeignKeyConstraints() {
        println("my_own_foreign_key_query")
        jdbc.fetchAllForeignKeyConstraintsFor("public","test").toTableString(
                headers = listOf("Source Column Name", "Destination Column Name", "Constraint Name")
        ) { (src, dest, consName) ->
            listOf(src.toString(), dest.toString(), consName.toString())
        }.println()
    }

    @Test
    fun collectColumnInfos() {
        println("my_own_columns")
        jdbc.fetchAllColumnInfosFor("public", "test").toTableString(
                headers = listOf("Column Name", "Data Type", "Is Nullable")
        ) { (columnName, dataType, isNullable) ->
            listOf(columnName.toString(), dataType.toString(), if(isNullable) "YES" else "NO")
        }.println()
    }


    @Test @Disabled("only needed for development")
    fun testInformationSchema() {

        println("key_column_usage")
        jdbc.query("""
            select * from information_schema.key_column_usage
            where table_catalog='PUBLIC' and table_schema='TEST' order by table_name, ordinal_position
            """.trimIndent(), ToTableStringResultSetExtractor).println()


        println("table_constraints")
        jdbc.query("""
            select * from information_schema.table_constraints
            where table_catalog='PUBLIC' and table_schema='TEST' order by table_name
            """.trimIndent(), ToTableStringResultSetExtractor).println()

        println("REFERENTIAL_CONSTRAINTS")
        jdbc.query("""
            select * from information_schema.REFERENTIAL_CONSTRAINTS
            """.trimIndent(), ToTableStringResultSetExtractor).println()
//            where table_catalog='PUBLIC' and table_schema='TEST' order by table_name

        println("constraint_column_usage")
        jdbc.query("""
            select * from information_schema.constraint_column_usage
            where table_catalog='PUBLIC' and table_schema='TEST' order by table_name
            """.trimIndent(), ToTableStringResultSetExtractor).println()

        println("check_constraints")
        jdbc.query("""
            select * from information_schema.check_constraints
            where constraint_catalog='PUBLIC' and constraint_schema='TEST'
            """.trimIndent(), ToTableStringResultSetExtractor).println()

    }
}
