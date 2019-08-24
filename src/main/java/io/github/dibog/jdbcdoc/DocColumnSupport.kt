package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName

class DocColumnSupport(private val parent: DocTableSupport, private val columnName: String) {

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
    }

    fun complete() {}
}