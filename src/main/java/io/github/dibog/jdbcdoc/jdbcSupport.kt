package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import java.sql.ResultSet

internal fun JdbcTemplate.createDatabase() {
    execute("""
        create schema test;
    """.trimIndent())
}

fun ResultSet.dump() {
    val md = this.metaData
    println("Header: "+(1..md.columnCount).map { md.getColumnName(it) }.joinToString(", ") { "\"$it\"" })
    while(next()) {
        println("Data: "+(1..md.columnCount).map { getObject(it) }.joinToString(",") { "\"$it\"" })
    }
}

fun ResultSet.toTableString(): String {
    val meta = metaData
    val headers = (1..meta.columnCount).map { meta.getColumnLabel(it) }.toList()
    return mapToList { rs ->
        (1..meta.columnCount).map { rs.getString(it) }
    }.toTableString(
            headers = headers
    ) { it }
}

fun <R> ResultSet.mapToIterator( action: (ResultSet)->R): Iterator<R> {
    val rs = this
    return iterator {
        while(rs.next()) {
            yield(action(rs))
        }
    }
}

fun <R> ResultSet.mapToList( action: (ResultSet)->R): List<R> {
    val result = mutableListOf<R>()
    val iter = mapToIterator(action)
    while(iter.hasNext()) {
        result.add(iter.next())
    }
    return result
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

object ToTableStringResultSetExtractor : ResultSetExtractor<String> {
    override fun extractData(rs: ResultSet) = rs.toTableString()
}
