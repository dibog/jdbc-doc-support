package io.github.dibog.jdbcdoc

import org.springframework.jdbc.core.JdbcTemplate
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