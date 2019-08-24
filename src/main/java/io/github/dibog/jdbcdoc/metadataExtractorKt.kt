package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.*
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import java.lang.IllegalStateException
import java.sql.ResultSet


fun JdbcTemplate.fetchAllPrimaryKeyConstraintsOf(catalog: String, schema: String): Set<PrimaryKeyConstraint> {
    return fetchAllConstraintsOf(catalog, schema, "PRIMARY KEY")
            .map { it.toPrimaryKeyConstraint() }
            .toSet()
}

fun JdbcTemplate.fetchAllUniqueConstraintsOf(catalog: String, schema: String): Set<UniqueConstraint> {
    return fetchAllConstraintsOf(catalog, schema, "UNIQUE")
            .map { it.toUniqueConstraint() }
            .toSet()
}

private fun JdbcTemplate.fetchAllConstraintsOf(catalog: String, schema: String, type: String): Set<PKOrUniqueConstraint> {
    return query("""
            select tc.*, ccu.column_name
            from information_schema.table_constraints tc,
                 information_schema.constraint_column_usage ccu
            where tc.table_catalog=? and tc.table_schema=?   
              and tc.CONSTRAINT_TYPE=?
              and tc.CONSTRAINT_CATALOG=ccu.CONSTRAINT_CATALOG
              and tc.CONSTRAINT_SCHEMA=ccu.CONSTRAINT_SCHEMA
              and tc.CONSTRAINT_NAME=ccu.CONSTRAINT_NAME
            """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase(), type.toUpperCase()),
            ResultSetExtractor<Set<PKOrUniqueConstraint>> { rs ->
                val result = mutableMapOf<FullConstraintName, PKOrUniqueConstraint.Builder>()

                while(rs.next()) {
                    val fullConstraintName = rs.extractFullConstraintName()
                    val fullColumnName = rs.extractFullColumnName()
                    result.getOrPut(fullConstraintName,  { PKOrUniqueConstraint.Builder(fullConstraintName) })
                            .addColumnName(fullColumnName)
                }

                result.values.map { it.build() }.toSet()
            })
}

fun JdbcTemplate.fetchAllCheckConstraintsOf(catalog: String, schema: String): Set<CheckConstraint> {
    return query("""
            select ccu.*, cc.CHECK_CLAUSE
            from information_schema.check_constraints cc, 
                 information_schema.constraint_column_usage ccu
            where ccu.table_catalog=? and ccu.table_schema=?
              and ccu.CONSTRAINT_CATALOG=cc.CONSTRAINT_CATALOG and ccu.CONSTRAINT_SCHEMA=cc.CONSTRAINT_SCHEMA and ccu.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
        """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase()),
            ResultSetExtractor<Set<CheckConstraint>> { rs ->
                val result = mutableMapOf<FullConstraintName, CheckConstraint.Builder>()
                while(rs.next()) {
                    val fullConstraintName = rs.extractFullConstraintName()
                    val fullColumnName = rs.extractFullColumnName()
                    val checkClause = rs.getString("CHECK_CLAUSE")

                    result.getOrPut(fullConstraintName,  { CheckConstraint.Builder(fullConstraintName, checkClause) })
                            .addColumnName(fullColumnName)
                }

                result.values.map { it.build() }.toSet()
            }
    )
}

fun JdbcTemplate.fetchAllForeignKeyConstraintsFor(catalog: String, schema: String): Set<ForeignKeyConstraint> {
    val srcKeys = fetchAllConstraintsOf(catalog, schema, "FOREIGN KEY").associateBy { it.fullConstraintName }
    val destKeys = (fetchAllConstraintsOf(catalog, schema, "PRIMARY KEY")+
            fetchAllConstraintsOf(catalog, schema, "UNIQUE")).associateBy { it.fullConstraintName }

    return query("""
            select * 
            from information_schema.REFERENTIAL_CONSTRAINTS
            where CONSTRAINT_CATALOG=?
              and CONSTRAINT_SCHEMA=?
            """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase())
    ) { rs, _ ->
        val srcConstraintName = rs.extractFullConstraintName()
        val destConstraintName = rs.extractFullConstraintName("UNIQUE_")
        val src = srcKeys[srcConstraintName] ?: throw IllegalStateException("Unknown foreign key '$srcConstraintName'")
        val dest = destKeys[destConstraintName] ?: throw IllegalStateException("Unknown target key '$srcConstraintName'")

        ForeignKeyConstraint(srcConstraintName, src.fullColumnNames, dest.fullColumnNames)
    }.toSet()
}

fun ResultSet.extractFullTableName(prefix: String=""): FullTableName {
    val catalog = getString("${prefix}TABLE_CATALOG")
    val schema = getString("${prefix}TABLE_SCHEMA")
    val table = getString("${prefix}TABLE_NAME")
    return FullTableName(catalog, schema, table)
}

fun ResultSet.extractFullColumnName(prefix: String=""): FullColumnName {
    val fullTableName = extractFullTableName(prefix)
    val columnName = getString("${prefix}COLUMN_NAME")

    return FullColumnName(fullTableName, columnName)
}

fun ResultSet.extractFullConstraintName(prefix: String=""): FullConstraintName {
    val catalog = getString("${prefix}CONSTRAINT_CATALOG")
    val schema = getString("${prefix}CONSTRAINT_SCHEMA")
    val constraint = getString("${prefix}CONSTRAINT_NAME")
    return FullConstraintName(catalog, schema, constraint)
}

fun JdbcTemplate.fetchAllColumnInfosFor(catalog: String, schema: String): List<ColumnInfo> {
    return query("""
            select *
            from information_schema.columns
            where table_catalog=? and table_schema=?
            """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase())
    ) { rs, _ ->
        val fullColumnName = rs.extractFullColumnName()

        val dataType = rs.getString("DATA_TYPE")
        val isNullable = rs.getString("IS_NULLABLE")=="YES"

        ColumnInfo(fullColumnName, dataType, isNullable)
    }
}

object ToTableStringResultSetExtractor : ResultSetExtractor<String> {
    override fun extractData(rs: ResultSet) = rs.toTableString()
}

fun String.println() = println(this)




