package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.jdbc.core.JdbcTemplate

@TestInstance(PER_CLASS)
class TestUsage {
    private val jdbc = JdbcTemplate(setupDataSource()).apply {
        createDatabase(this)
        initializeDatabase(this)
    }

    private fun initializeDatabase(jdbc: JdbcTemplate) {

    jdbc.execute("""
    create table test.foo1 (
        id int not null,
        name varchar(20) not null,
        CONSTRAINT PK_FOO1 PRIMARY KEY (id),
        CONSTRAINT UC_FOO1_NAME UNIQUE (name)
    );
    """.trimIndent())

    jdbc.execute("""
    create table test.foo2 (
        id int not null,
        foo_id int not null,
        my_check varchar(20),
        CONSTRAINT FK_FOO2_ID FOREIGN KEY (foo_id) REFERENCES test.foo1(id),
        CONSTRAINT PK_FOO2 PRIMARY KEY (id),
        CONSTRAINT CH_CHECK CHECK (my_check!='foo' AND id<20)
    );
    """.trimIndent())

    jdbc.execute("""
    create table test.foo3 (
        id1 int not null,
        id2 int null,
        CONSTRAINT PK_FOO3 PRIMARY KEY (id1, id2),
        CONSTRAINT FK_FOO3_ID1 FOREIGN KEY (id1) REFERENCES test.foo1(id),
        CONSTRAINT FK_FOO3_ID2 FOREIGN KEY (id2) REFERENCES test.foo2(id),
    );
    """.trimIndent())
    }

    private val document = DocumentHelper(jdbc, "public", "test", context = Context(
            "^SYS_.*$".toRegex()
    ))

    @AfterAll
    fun shutdown() {
        jdbc.execute("SHUTDOWN")
    }

    @Test
    fun documentTableFoo1_variantA() {
        document.table("foo1", "foo1a") {
            column("id", "INTEGER", NOT_NULL) {
                isPrimaryKey()
                hasComment("My own comment for id")
            }

            column("name", "VARCHAR(20)", NOT_NULL) {
                isUnique()
                hasComment("My own comment for name")
            }
        }
    }

    @Test
    fun documentTableFoo1_variantB() {
        document.table("foo1", "foo1b") {
            column("id", "INTEGER", NOT_NULL)
            column("name", "VARCHAR(20)", NOT_NULL)

            primaryKey("PK_FOO1", "id")
            unique("UC_FOO1_NAME", "name")
        }
    }

    @Test
    fun documentTableFoo2() {
        document.table("foo2") {
            column("id", "INTEGER", NOT_NULL) {
                isPrimaryKey("PK_FOO2")
            }

            column("foo_id", "INTEGER", NOT_NULL) {
                foreignKey(null, "foo1", "id")
            }

            column("my_check", "VARCHAR(20)", NULL)

            check("CH_CHECK", listOf("id", "my_check"))
        }
    }

    @Test
    fun documentTableFoo3() {
        document.table("foo3") {
            column("id1", "INTEGER", NOT_NULL) {
                foreignKey("FK_FOO3_ID1", "foo1", "id")
            }

            column("id2", "INTEGER", NULL) {
                foreignKey("FK_FOO3_ID2", "foo2", "id")
            }

            primaryKey("PK_FOO3", listOf("id1", "id2"))
        }
    }

    @Test
    fun documentSchema() {
        document.schema()
    }
}
