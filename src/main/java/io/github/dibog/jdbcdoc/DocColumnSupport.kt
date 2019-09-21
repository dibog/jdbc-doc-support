package io.github.dibog.jdbcdoc


class DocColumnSupport(private val parent: DocTableSupport, private val columnName: String) {
    internal var comment: String? = null

    fun isPrimaryKey(constraintName: String? = null) {
        parent.primaryKey(constraintName, listOf(columnName))
    }

    fun isUnique(constraintName: String? = null) {
        parent.unique(constraintName, listOf(columnName))
    }

    fun foreignKey(constraintName: String?=null, targetTable: String, targetColumn: String) {
        parent.foreignKey(constraintName, columnName, targetTable, targetColumn)
    }


    fun hasComment(comment: String) {
        this.comment = comment
    }

    fun complete() {}
}