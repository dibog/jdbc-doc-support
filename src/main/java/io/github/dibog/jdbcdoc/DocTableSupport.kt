package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.CheckVerifier
import io.github.dibog.jdbcdoc.entities.ColumnVerifier
import io.github.dibog.jdbcdoc.entities.ForeignKeyVerifier
import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.PrimaryKeyVerifier
import io.github.dibog.jdbcdoc.entities.TableVerifier
import io.github.dibog.jdbcdoc.entities.UniqueVerifier

const val NULL = true
const val NOT_NULL = false

class DocTableSupport(private val helper: DocumentHelper, private val snippetName: String, tableName: String) {

//    private val tableUserInfo : TableUserInfo
    private val verifier : TableVerifier

    init {
        val tableInfo = helper.fetchTable(tableName) ?: throw IllegalArgumentException("Unknown table '$tableName'")
        verifier = TableVerifier(
                tableInfo.tableName,
                tableInfo.primaryKey?.let { PrimaryKeyVerifier(it) },
                tableInfo.columns.map { ColumnVerifier(it) },
                tableInfo.uniques.map { UniqueVerifier(it) },
                tableInfo.checks.map { CheckVerifier(it) },
                tableInfo.foreignKeys.map { ForeignKeyVerifier(it) }
        )
    }

    fun column(columnName: String, expectedDataType: String, expectedNullability: Boolean, action: DocColumnSupport.()->Unit = {}) {
        val support = DocColumnSupport(this, columnName)
        support.action()
        support.complete()

        documentColumn(columnName, expectedDataType, expectedNullability, support.comment)
    }

    private fun documentColumn(name: String, expectedDataType: String, expectedNullability: Boolean, comment: String? = null) {
        verifier.documentColumn(name, expectedDataType, expectedNullability, comment)
    }


    fun check(constraintName: String, columns: List<String>, clause: String? = null) {
        verifier.documentCheckConstraint(constraintName, columns, clause)
    }

    fun foreignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        verifier.documentForeignKey(constraintName, srcColumn, targetTable, targetColumn)
    }

    fun foreignKey(constraintName: String? = null, expectedColumns: Map<FullColumnName, FullColumnName>) {
        verifier.documentForeignKey(constraintName, expectedColumns)
    }

    fun primaryKey(constraintName: String?=null, expectedColumn: String) {
        primaryKey(constraintName, listOf(expectedColumn))
    }

    fun primaryKey(constraintName: String?=null, expectedColumns: List<String>) {
        verifier.documentPrimaryKey(constraintName, expectedColumns)
    }

    fun unique(constraintName: String?=null, expectedColumn: String) {
        unique(constraintName, listOf(expectedColumn))
    }

    fun unique(constraintName: String?=null, expectedColumns: List<String>) {
        verifier.documentUnique(constraintName, expectedColumns)
    }

    fun complete(skipCheckedException: Boolean = true) {
        verifier.documentAll()
        val errors = verifier.verifyAll(skipCheckedException)
        verifier.createDocumentation(snippetName)

        if(errors.isNotBlank()) {
            throw AssertionError(errors)
        }
    }
}
