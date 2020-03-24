package io.github.dibog.jdbcdoc


class DocColumnSupport(private val parent: DocTableSupport, private val columnName: String) {
    private var comment: String? = null

    fun isPrimaryKey(constraintName: String? = null) {
        parent.primaryKey(constraintName, listOf(columnName))
    }

    fun isUnique(constraintName: String? = null) {
        parent.unique(constraintName, listOf(columnName))
    }

    fun foreignKey(constraintName: String?=null, targetTable: String, targetColumn: String) {
        parent.foreignKey(constraintName, columnName, targetTable, targetColumn)
    }

    fun check(constraintName: String, clause: String? = null) {
        parent.check(constraintName, columnName, clause)
    }

    fun hasComment(comment: String) {
        this.comment = comment
    }

    fun complete(): String? {
        return comment
    }
}