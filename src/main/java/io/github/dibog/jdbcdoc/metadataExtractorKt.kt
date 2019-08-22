package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName
import io.github.dibog.jdbcdoc.entities.TmpCheckConstraint
import io.github.dibog.jdbcdoc.entities.TmpColumnInfo
import io.github.dibog.jdbcdoc.entities.TmpForeignKeyConstraint
import io.github.dibog.jdbcdoc.entities.TmpPKOrUniqueConstraint
import io.github.dibog.jdbcdoc.entities.TmpPrimaryKeyConstraint
import io.github.dibog.jdbcdoc.entities.TmpUniqueConstraint
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import java.sql.ResultSet

private fun JdbcTemplate.fetchAllPkOrUniqueConstraintsOf(catalog: String, schema: String, type: String): List<TmpPKOrUniqueConstraint> {
    return query("""
            select tc.*, ccu.column_name
            from information_schema.table_constraints tc,
                 information_schema.constraint_column_usage ccu
            where tc.table_catalog=? and tc.table_schema=?   
              and tc.CONSTRAINT_TYPE=?
              and tc.CONSTRAINT_CATALOG=ccu.CONSTRAINT_CATALOG
              and tc.CONSTRAINT_SCHEMA=ccu.CONSTRAINT_SCHEMA
              and tc.CONSTRAINT_NAME=ccu.CONSTRAINT_NAME
            """.trimIndent()
            , arrayOf(catalog.toUpperCase(), schema.toUpperCase(), type.toUpperCase())
    ) { rs, _ ->
        val fullColumnName = rs.extractFullColumnName()
        val fullConstraintName = rs.extractFullConstraintName()

        TmpPKOrUniqueConstraint(fullColumnName, fullConstraintName)
    }
}

fun JdbcTemplate.fetchAllUniqueConstraintsOf(catalog: String, schema: String): List<TmpUniqueConstraint> {
    return fetchAllPkOrUniqueConstraintsOf(catalog, schema, "unique").map { it.toUniqueConstraint() }
}

fun JdbcTemplate.fetchAllPrimaryKeyConstraintsOf(catalog: String, schema: String): List<TmpPrimaryKeyConstraint> {
    return fetchAllPkOrUniqueConstraintsOf(catalog, schema, "primary key").map { it.toPrimaryKeyConstraint() }
}

fun JdbcTemplate.fetchAllCheckConstraintsOf(catalog: String, schema: String): List<TmpCheckConstraint> {
    return query("""
            select ccu.*, cc.CHECK_CLAUSE
            from information_schema.check_constraints cc, 
                 information_schema.constraint_column_usage ccu
            where ccu.table_catalog=? and ccu.table_schema=?
              and ccu.CONSTRAINT_CATALOG=cc.CONSTRAINT_CATALOG and ccu.CONSTRAINT_SCHEMA=cc.CONSTRAINT_SCHEMA and ccu.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
        """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase())
    ) { rs, _ ->
        val fullColumnName = rs.extractFullColumnName()
        val fullConstraintName = rs.extractFullConstraintName()
        val checkClause = rs.getString("CHECK_CLAUSE")

        TmpCheckConstraint(fullColumnName, fullConstraintName, checkClause)
    }
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

fun JdbcTemplate.fetchAllColumnInfosFor(catalog: String, schema: String): List<TmpColumnInfo> {
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

        TmpColumnInfo(fullColumnName, dataType, isNullable)
    }
}

fun JdbcTemplate.fetchAllForeignKeyConstraintsFor(catalog: String, schema: String): List<TmpForeignKeyConstraint> {
    return query("""
            select 
                   ccu.TABLE_CATALOG SRC_TABLE_CATALOG,
                   ccu.TABLE_SCHEMA SRC_TABLE_SCHEMA,
                   ccu.TABLE_NAME as SRC_TABLE_NAME, 
                   ccu.COLUMN_NAME as SRC_COLUMN_NAME, 
                   ccu.CONSTRAINT_CATALOG CONSTRAINT_CATALOG,
                   ccu.CONSTRAINT_SCHEMA CONSTRAINT_SCHEMA,
                   ccu.CONSTRAINT_NAME CONSTRAINT_NAME,
                   kcu.TABLE_CATALOG DEST_TABLE_CATALOG,
                   kcu.TABLE_SCHEMA DEST_TABLE_SCHEMA,
                   kcu.TABLE_NAME DEST_TABLE_NAME,
                   kcu.COLUMN_NAME DEST_COLUMN_NAME
            from information_schema.constraint_column_usage ccu,
                 information_schema.REFERENTIAL_CONSTRAINTS rc,
                 information_schema.KEY_COLUMN_USAGE kcu
            where ccu.table_catalog=? and ccu.table_schema=?
              and ccu.CONSTRAINT_CATALOG=rc.CONSTRAINT_CATALOG
              and ccu.CONSTRAINT_SCHEMA=rc.CONSTRAINT_SCHEMA
              and ccu.CONSTRAINT_NAME=rc.CONSTRAINT_NAME
              and kcu.CONSTRAINT_CATALOG=rc.UNIQUE_CONSTRAINT_CATALOG
              and kcu.CONSTRAINT_SCHEMA=rc.UNIQUE_CONSTRAINT_SCHEMA
              and kcu.CONSTRAINT_NAME=rc.UNIQUE_CONSTRAINT_NAME
            """.trimIndent(),
            arrayOf(catalog.toUpperCase(), schema.toUpperCase())
    ) { rs, _ ->
        val fullSrcColumnName = rs.extractFullColumnName("SRC_")
        val fullConstraintName = rs.extractFullConstraintName()
        val fullDestColumnName = rs.extractFullColumnName("DEST_")

        TmpForeignKeyConstraint(fullSrcColumnName, fullDestColumnName, fullConstraintName)
    }
}

object ToTableStringResultSetExtractor : ResultSetExtractor<String> {
    override fun extractData(rs: ResultSet) = rs.toTableString()
}

fun String.println() = println(this)




