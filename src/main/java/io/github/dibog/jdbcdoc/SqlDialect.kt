package io.github.dibog.jdbcdoc

import java.lang.IllegalStateException
import java.sql.Connection

abstract class SqlDialect {
    companion object {
        fun createDialect(conn: Connection): SqlDialect {
            return conn.use {
                when(conn.metaData.databaseProductName) {
                    "PostgreSQL" -> PostgresDialect
                    "HSQL Database Engine" -> HSqlDbDialect

                    else -> throw IllegalStateException("Unsupported database ${conn.metaData.databaseProductName}")
                }
            }
        }
    }

    protected abstract fun String.quoteIfNecessary(): String

    fun catalog(catalog: String) =  catalog.quoteIfNecessary()
    fun schema(schema: String) = schema.quoteIfNecessary()
    fun table(table: String) = table.quoteIfNecessary()
    fun column(column: String) = column.quoteIfNecessary()
    fun constraint(constraint: String) = constraint.quoteIfNecessary()
}

object HSqlDbDialect : SqlDialect() {
    private val String.isQuoted
        get() = startsWith("\"") && this.endsWith("\"")

    private val String.requiresQuoting
        get() = !isQuoted && contains("\"")

    override fun String.quoteIfNecessary(): String {
        return if(requiresQuoting) {
            val quote = replace("\"", "\\\"")
            return "\""+quote+"\""
        }
        else {
            this.toUpperCase()
        }
    }
}

object PostgresDialect : SqlDialect() {
    private val String.isQuoted
            get() = startsWith("\"") && this.endsWith("\"")

    private val String.requiresQuoting
            get() = !isQuoted && contains("\"")

    override fun String.quoteIfNecessary(): String {
        return if(requiresQuoting) {
            val quote = replace("\"", "\\\"")
            return "\""+quote+"\""
        }
        else {
            this.toLowerCase()
        }
    }
}

fun main() {

}
