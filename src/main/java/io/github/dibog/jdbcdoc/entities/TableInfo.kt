package io.github.dibog.jdbcdoc.entities

data class TableInfo(val name: FullTableName, val columns: List<TmpColumnInfo>, val constraints: List<Any>) {

    fun columnNullability(name: FullColumnName): Boolean {
        val column = columns.first { it.columnName==name }
        return column.isNullable
    }

    fun columnDataType(name: FullColumnName): String {
        val column = columns.first { it.columnName==name }
        return column.dataType
    }

}