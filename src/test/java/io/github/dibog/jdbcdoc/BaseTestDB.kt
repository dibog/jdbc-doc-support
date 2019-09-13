package io.github.dibog.jdbcdoc

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate

abstract class BaseTestDB(private val catalog: String, protected val schema: String, protected val jdbc: JdbcTemplate) {

    init {
        initializeDatabase()
    }

    private val extractor = DatabaseInspector(jdbc, catalog, schema)

    private fun initializeDatabase() {

        jdbc.execute("""
        create table $schema.foo1 (
            id int not null,
            name varchar(20) not null,
            CONSTRAINT PK_FOO1 PRIMARY KEY (id),
            CONSTRAINT UC_FOO1_NAME UNIQUE (name)
        );
        """.trimIndent())

        jdbc.execute("""
        create table $schema.foo2 (
            id int not null,
            foo_id int not null,
            my_check varchar(20),
            CONSTRAINT FK_FOO2_ID FOREIGN KEY (id) REFERENCES test.foo1(id),
            CONSTRAINT PK_FOO2 PRIMARY KEY (id),
            CONSTRAINT CH_CHECK CHECK (my_check!='foo' AND id<20)
        );
        """.trimIndent())

        jdbc.execute("""
        create table $schema.foo3 (
            id1 int not null,
            id2 int not null,
            uq1 int NOT NULL,
	        uq2 varchar(20) NOT NULL,
            CONSTRAINT PK_FOO3 PRIMARY KEY (id1, id2),
            CONSTRAINT UQ_FOO3 UNIQUE (uq1, uq2),
            CONSTRAINT FK_FOO3_ID1 FOREIGN KEY (id1) REFERENCES test.foo1(id),
            CONSTRAINT FK_FOO3_ID2 FOREIGN KEY (id2) REFERENCES test.foo2(id)
        );
        """.trimIndent())

        jdbc.execute("""
        create table $schema.foo4 (
            id1 int not null,
            id2 int not null,
            id3 varchar(20) not null,
            id4 int not null,
            id5 varchar(20) not null,
            id6 int not null,
            CONSTRAINT PK_FOO4 PRIMARY KEY (id1, id2),
            CONSTRAINT UQ_FOO4_1 UNIQUE (id3, id4),
            CONSTRAINT UQ_FOO4_2 UNIQUE (id6, id5),
            CONSTRAINT FK_FOO4_ID1 FOREIGN KEY (id1,id2) REFERENCES test.foo3(id1,id2),
            CONSTRAINT FK_FOO4_ID2 FOREIGN KEY (id3,id4) REFERENCES test.foo3(uq2,uq1),
            CONSTRAINT FK_FOO4_ID3 FOREIGN KEY (id6,id5) REFERENCES test.foo3(uq1,uq2)
        );
        """.trimIndent())
    }


    @Test
    fun collectCheckConstraints() {
        val checkConstraints = extractor.checks.flatMap { it.value }
        val withoutSysChecks = checkConstraints.filter {
            !it.constraintName.constraint.startsWith("SYS_")
        }.toSet()

        assertThat(withoutSysChecks).hasSize(1)
        val cc = withoutSysChecks.iterator().next()
        assertThat(cc.constraintName).isEqualTo(extractor.toFullConstraintName("CH_CHECK"))
        assertThat(cc.columnNames).isEqualTo(
                listOf(
                        extractor.toFullColumnName("foo2", "id"),
                        extractor.toFullColumnName("foo2", "MY_CHECK")
                )
        )
    }

    @Test
    fun collectPrimaryKeys() {
        val primaryKeys = extractor.primaryKeys.flatMap { it.value }.toSet()

        assertThat(primaryKeys).isEqualTo(
                setOf(
                        PrimaryKeyConstraint(
                                extractor.toFullConstraintName("pk_foo1"),
                                extractor.toFullColumnName("foo1", "id")),
                        PrimaryKeyConstraint(
                                extractor.toFullConstraintName("pk_foo2"),
                                extractor.toFullColumnName("foo2", "id")),
                        PrimaryKeyConstraint(
                                extractor.toFullConstraintName("pk_foo3"),
                                listOf(
                                        extractor.toFullColumnName("foo3", "id1"),
                                        extractor.toFullColumnName("foo3", "id2"))),
                        PrimaryKeyConstraint(
                                extractor.toFullConstraintName("pk_foo4"),
                                listOf(
                                        extractor.toFullColumnName("foo4", "id1"),
                                        extractor.toFullColumnName("foo4", "id2")))
                )
        )
    }

    @Test
    fun collectUniqueConstraints() {
        val unique = extractor.uniques.flatMap { it.value }.toSet()

        assertThat(unique).isEqualTo(
                setOf(
                        UniqueConstraint(
                                extractor.toFullConstraintName("uc_foo1_name"),
                                extractor.toFullColumnName("foo1", "name")),
                        UniqueConstraint(
                                extractor.toFullConstraintName("uq_foo3"),
                                listOf(
                                        extractor.toFullColumnName("foo3", "uq1"),
                                        extractor.toFullColumnName("foo3", "uq2"))),
                        UniqueConstraint(
                                extractor.toFullConstraintName("uq_foo4_1"),
                                listOf(
                                        extractor.toFullColumnName("foo4", "id3"),
                                        extractor.toFullColumnName("foo4", "id4"))),
                        UniqueConstraint(
                                extractor.toFullConstraintName("uq_foo4_2"),
                                listOf(
                                        extractor.toFullColumnName("foo4", "id6"),
                                        extractor.toFullColumnName("foo4", "id5")))
                )
        )
    }

    @Test
    fun collectForeignKeyConstraints() {
        val foreignKeys = extractor.foreignKeys.flatMap { it.value }.toSet()

        assertThat(foreignKeys).isEqualTo(
                setOf(
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO2_ID"),
                                extractor.toFullColumnName("FOO2", "ID"),
                                extractor.toFullColumnName("FOO1", "ID")
                        ),
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO3_ID1"),
                                extractor.toFullColumnName("FOO3", "ID1"),
                                extractor.toFullColumnName("FOO1", "ID")
                        ),
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO3_ID2"),
                                extractor.toFullColumnName("FOO3", "ID2"),
                                extractor.toFullColumnName("FOO2", "ID")
                        ),
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO4_ID1"),
                                extractor.toFullColumnNames(
                                        "FOO4", listOf("ID1","ID2"),
                                        "FOO3", listOf("ID1","ID2"))
                        ),
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO4_ID2"),
                                extractor.toFullColumnNames(
                                        "FOO4", listOf("ID3", "ID4"),
                                        "FOO3", listOf("UQ2", "UQ1")
                                )
                        ),
                        ForeignKeyConstraint(
                                extractor.toFullConstraintName("FK_FOO4_ID3"),
                                extractor.toFullColumnNames(
                                        "FOO4", listOf("ID6", "ID5"),
                                        "FOO3", listOf("UQ1", "UQ2")
                                )
                        )
                )
        )
    }

    @Test
    @Disabled
    fun collectColumnInfos() {
        println("my_own_columns")
        extractor.columns.flatMap { it.value }.toTableString(
                headers = listOf("Column Name", "Data Type", "Is Nullable")
        ) { (columnName, dataType, isNullable) ->
            listOf(columnName.toString(), dataType.toString(), if (isNullable) "YES" else "NO")
        }.println()
    }


    @Test
    @Disabled("only needed for development")
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
