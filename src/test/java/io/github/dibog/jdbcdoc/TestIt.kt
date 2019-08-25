package io.github.dibog.jdbcdoc

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
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
            uq1 int NOT NULL,
	        uq2 varchar(20) NOT NULL,
            CONSTRAINT PK_FOO3 PRIMARY KEY (id1, id2),
            CONSTRAINT UQ_FOO3 UNIQUE (uq1, uq2),
            CONSTRAINT FK_FOO3_ID1 FOREIGN KEY (id1) REFERENCES test.foo1(id),
            CONSTRAINT FK_FOO3_ID2 FOREIGN KEY (id2) REFERENCES test.foo2(id),
        );
        """.trimIndent())

        execute("""
        create table test.foo4 (
            id1 int not null,
            id2 int not null,
            id3 varchar(20) not null,
            id4 int not null,
            CONSTRAINT PK_FOO4 PRIMARY KEY (id1, id2),
            CONSTRAINT UQ_FOO4 UNIQUE (id4, id3),
            CONSTRAINT FK_FOO4_ID1 FOREIGN KEY (id1,id2) REFERENCES test.foo3(id1,id2),
            CONSTRAINT FK_FOO4_ID2 FOREIGN KEY (id3,id4) REFERENCES test.foo3(uq2,uq1)
        );
        """.trimIndent())
    }

    private val extractor = Extractor(jdbc, "public", "test")

    @AfterAll
    fun shutdown() {
        jdbc.execute("SHUTDOWN")
    }

    @Test
    fun collectCheckConstraints() {
        val checkConstraints = extractor.checks.flatMap { it.value }
        val withoutSysChecks = checkConstraints.filter { !it.constraintName.constraint.startsWith("SYS_") }.toSet()

        assertThat(withoutSysChecks).isEqualTo(
                setOf(
                        CheckConstraint(
                                FullConstraintName("PUBLIC", "TEST", "CH_CHECK"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO2", "ID"),
                                        FullColumnName("PUBLIC", "TEST", "FOO2", "MY_CHECK")),
                                "(TEST.FOO2.MY_CHECK!='foo') AND (TEST.FOO2.ID<20)"
                        )
                )
        )
    }

    @Test
    fun collectPrimaryKeys() {
        val primaryKeys = extractor.primaryKeys.flatMap { it.value }.toSet()

        assertThat(primaryKeys).isEqualTo(
                setOf(
                        PrimaryKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "PK_FOO1"),
                                FullColumnName("PUBLIC", "TEST", "FOO1", "ID")),
                        PrimaryKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "PK_FOO2"),
                                FullColumnName("PUBLIC", "TEST", "FOO2", "ID")),
                        PrimaryKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "PK_FOO3"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "ID1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "ID2"))),
                        PrimaryKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "PK_FOO4"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID2")))
                )
        )
    }

    @Test
    fun collectUniqueConstraints() {
        val unique = extractor.uniques.flatMap { it.value }.toSet()

        assertThat(unique).isEqualTo(
                setOf(
                        UniqueConstraint(
                                FullConstraintName("PUBLIC", "TEST", "UC_FOO1_NAME"),
                                FullColumnName("PUBLIC", "TEST", "FOO1", "NAME")),
                        UniqueConstraint(
                                FullConstraintName("PUBLIC", "TEST", "UQ_FOO3"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "UQ1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "UQ2"))),
                        UniqueConstraint(
                                FullConstraintName("PUBLIC", "TEST", "UQ_FOO4"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID4"),
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID3")))
                )
        )
    }

    @Test
    fun collectForeignKeyConstraints() {
        val foreignKeys = extractor.foreignKeys.flatMap { it.value }.toSet()

        assertThat(foreignKeys).isEqualTo(
                setOf(
                        ForeignKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "FK_FOO2_ID"),
                                FullColumnName("PUBLIC", "TEST", "FOO2", "ID"),
                                FullColumnName("PUBLIC", "TEST", "FOO1", "ID")
                        ),
                        ForeignKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "FK_FOO3_ID1"),
                                FullColumnName("PUBLIC", "TEST", "FOO3", "ID1"),
                                FullColumnName("PUBLIC", "TEST", "FOO1", "ID")
                        ),
                        ForeignKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "FK_FOO3_ID2"),
                                FullColumnName("PUBLIC", "TEST", "FOO3", "ID2"),
                                FullColumnName("PUBLIC", "TEST", "FOO2", "ID")
                        ),
                        ForeignKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "FK_FOO4_ID1"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID2")
                                ),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "ID1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "ID2")
                                )
                        ),
                        ForeignKeyConstraint(
                                FullConstraintName("PUBLIC", "TEST", "FK_FOO4_ID2"),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID4"),
                                        FullColumnName("PUBLIC", "TEST", "FOO4", "ID3")
                                ),
                                listOf(
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "UQ1"),
                                        FullColumnName("PUBLIC", "TEST", "FOO3", "UQ2")
                                )
                        )


                )
        )
    }

    @Test @Disabled
    fun collectColumnInfos() {
        println("my_own_columns")
        extractor.columns.flatMap { it.value }.toTableString(
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
