package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.*
import java.nio.file.Files
import java.nio.file.Path

const val NULL = true
const val NOT_NULL = false

class DocTableSupport(private val helper: DocumentHelper, tableName: String, private val docFolder: Path) {

    private val verifier : TableVerifier

    init {
        Files.createDirectories(docFolder)
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
        documentColumn(columnName, expectedDataType, expectedNullability)

        val support = DocColumnSupport(this, columnName)
        support.action()
        support.complete()
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

    fun foreignKey(constraintName: String? = null, expectedColumns: List<Pair<FullColumnName, FullColumnName>>) {
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
        verifier.createDocumentation(docFolder)

        if(errors.isNotBlank()) {
            throw AssertionError(errors)
        }
    }
}
