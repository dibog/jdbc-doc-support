package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.springframework.jdbc.core.JdbcTemplate
import java.nio.file.Path
import java.nio.file.Paths

@TestInstance(PER_CLASS)
class TestUsage {
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
        CONSTRAINT FK_FOO2_ID FOREIGN KEY (foo_id) REFERENCES test.foo1(id),
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

    private val docHelper = DocumentHelper(jdbc, "public", "test")

    @AfterAll
    fun shutdown() {
        jdbc.execute("SHUTDOWN")
    }

    @Test
    fun documentTableFoo1_variantA() {
        document("foo1") {
            column("id", "INTEGER", NOT_NULL) {
                isPrimaryKey()
                hasComment("My own comment for id")
            }

            column("name", "CHARACTER VARYING", NOT_NULL) {
                isUnique()
                hasComment("My own comment for name")
            }
        }
    }

    @Test
    fun documentTableFoo1_variantB() {
        document("foo1") {
            column("id", "INTEGER", NOT_NULL)
            column("name", "CHARACTER VARYING", NOT_NULL)

            primaryKey("PK_FOO1", "id")
            unique("UC_FOO1_NAME", "name")
        }
    }

    @Test
    fun documentTableFoo2() {
        document("foo2") {
            column("id", "INTEGER", NOT_NULL) {
                isPrimaryKey("PK_FOO2")
            }

            column("foo_id", "INTEGER", NOT_NULL) {
//                foreignKey("FK_FOO2_ID", "foo1", "id")
                foreignKey(null, "foo1", "id")
            }

            column("my_check", "CHARACTER VARYING", NULL) {
                // check constraint
            }

            check("CH_CHECK", listOf("id", "my_check"))
        }
    }

    private fun document(tableName: String, action: DocTableSupport.()->Unit = {}) {
        val support = DocTableSupport(docHelper, tableName, Paths.get("."))
        support.action()
        support.complete()
    }
}
