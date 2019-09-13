package io.github.dibog.jdbcdoc.entities

data class FullTableName(val catalog: String, val schema: String, val table: String) {
    fun toFullColumnName(column: String): FullColumnName {
        return FullColumnName(catalog, schema, table, column.toUpperCase())
    }
    fun toFullConstraintName(constraintName: String): FullConstraintName {
        return FullConstraintName(catalog, schema, constraintName.toUpperCase())
    }

    fun toFileName(): String {
        return "$catalog.$schema.$table"
    }

    override fun toString() = "$catalog.$schema.$table"

}
