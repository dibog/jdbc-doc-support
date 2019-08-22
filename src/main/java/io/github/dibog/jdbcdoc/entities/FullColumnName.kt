package io.github.dibog.jdbcdoc.entities

data class FullColumnName(val catalog: String, val schema: String, val table: String, val column: String) {
    constructor(fullTableName: FullTableName, column: String)
            : this(fullTableName.catalog, fullTableName.schema, fullTableName.table, column)

    val fullTableName = FullTableName(catalog, schema, table)
    override fun toString() = "$catalog.$schema.$table.$column"
}