package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName

class DocColumnSupport(private val parent: DocTableSupport, private val columnName: String) {
    private val fullColumnName = parent.toFullColumnName(columnName)

    fun isPrimaryKey(constraintName: String? = null) {
        parent.primaryKey(constraintName, setOf(columnName))
    }

    fun isUnique(constraintName: String? = null) {
        parent.unique(constraintName, setOf(columnName))
    }


    private fun foreignKey(targetTable: String, targetColumn: String, constraintName: String?=null) {
        parent.foreignKey(constraintName, fullColumnName, targetTable, targetColumn)
    }


    fun hasComment(comment: String) {
    }

    fun complete() {}
}