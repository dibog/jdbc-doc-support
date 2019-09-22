package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.TableVerifier

const val NULL = true
const val NOT_NULL = false

class DocTableSupport(private val helper: DocumentHelper, private val snippetName: String, tableName: String, private val context: Context) {

    private val tableInfo = helper.fetchTable(tableName) ?: throw IllegalArgumentException("Unknown table '$tableName'")
    private val userTableBuilder = TableUserInfoBuilder(tableInfo)

    fun column(columnName: String, expectedDataType: String, expectedNullability: Boolean, action: DocColumnSupport.()->Unit = {}) {
        val support = DocColumnSupport(this, columnName)
        support.action()
        val comment = support.complete()

        documentColumn(columnName, expectedDataType, expectedNullability, comment)
    }

    private fun documentColumn(name: String, expectedDataType: String, expectedNullability: Boolean, comment: String? = null) {
        userTableBuilder.addColumnInfo(name, expectedDataType, expectedNullability, comment)
    }


    fun check(constraintName: String, columns: List<String>, clause: String? = null) {
        userTableBuilder.addCheckConstraint(constraintName, columns, clause)
    }

    fun foreignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        userTableBuilder.documentForeignKey(constraintName, srcColumn, targetTable, targetColumn)
    }

    fun foreignKey(constraintName: String? = null, expectedColumns: Map<FullColumnName, FullColumnName>) {
        userTableBuilder.documentForeignKey(constraintName, expectedColumns)
    }

    fun primaryKey(constraintName: String?=null, expectedColumn: String) {
        primaryKey(constraintName, listOf(expectedColumn))
    }

    fun primaryKey(constraintName: String?=null, expectedColumns: List<String>) {
        userTableBuilder.documentPrimaryKey(constraintName, expectedColumns)
    }

    fun unique(constraintName: String?=null, expectedColumn: String) {
        unique(constraintName, listOf(expectedColumn))
    }

    fun unique(constraintName: String?=null, expectedColumns: List<String>) {
        userTableBuilder.documentUniqueKey(constraintName, expectedColumns)
    }

    fun complete(skipCheckedException: Boolean = true) {
        val userTable = userTableBuilder.build()
        val tableVerifier = TableVerifier(tableInfo, userTable, context)
        val errors = tableVerifier.verifyAll()

        if(errors.isNotBlank()) {
            throw AssertionError(errors)
        }

        val tableDocumenter = TableDocumenter(tableInfo, userTable)
        tableDocumenter.createDocumentation(snippetName)
    }
}
