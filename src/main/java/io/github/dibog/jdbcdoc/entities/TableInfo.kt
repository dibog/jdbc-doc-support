package io.github.dibog.jdbcdoc.entities

data class TableInfo(val name: FullTableName, val columns: List<ColumnInfo>, val constraints: List<Any>) {

    fun columnNullability(name: FullColumnName): Boolean {
        val column = columns.first { it.fullColumnName==name }
        return column.isNullable
    }

    fun columnDataType(name: FullColumnName): String {
        val column = columns.first { it.fullColumnName==name }
        return column.dataType
    }

}