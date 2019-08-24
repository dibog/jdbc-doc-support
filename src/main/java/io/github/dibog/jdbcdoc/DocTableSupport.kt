package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.*

const val NULL = true
const val NOT_NULL = false

class DocTableSupport(private val helper: DocumentHelper, tableName: String) {

    private val verifier : TableVerifier

    init {
        val tableInfo = helper.fetchTable(tableName) ?: throw IllegalArgumentException("Unknown table '$tableName'")
        val pkConstraints = tableInfo.constraints
                .filterIsInstance<PrimaryKeyConstraint>()
                .map { (name, columns) ->
                    PrimaryKeyConstraintChecker(name, columns)
                }
        require(pkConstraints.size<=1)

        val builder = TableVerifier.Builder(tableInfo.name, pkConstraints.firstOrNull())
        tableInfo.constraints
                .filterIsInstance<UniqueConstraint>()
                .forEach { (name, columns) ->
                    builder.addUniqueVerifier(name, columns)
                }

        tableInfo.constraints
                .filterIsInstance<CheckConstraint>()
                .forEach { (name, columns, clause)  ->
                    builder.addCheckVerifier(name, columns, clause)
                }

        tableInfo.constraints
                .filterIsInstance<ForeignKeyConstraint>()
                .forEach { (name, srcColumns, destColumns) ->
                    builder.addForeignKeyVerifier(name, srcColumns, destColumns)
                }

        tableInfo.columns
                .forEach { (name, dataType, isNullable) ->
                    builder.addColumnVerifier(name, dataType, isNullable )
                }

        verifier = builder.build()
    }

    fun column(columnName: String, expectedDataType: String, expectedNullability: Boolean, action: DocColumnSupport.()->Unit = {}) {
        checkColumn(columnName, expectedDataType, expectedNullability)

        val support = DocColumnSupport(this, columnName)
        support.action()
        support.complete()
    }

    private fun checkColumn(name: String, expectedDataType: String, expectedNullability: Boolean) {
        verifier.verifyColumn(name, expectedDataType, expectedNullability)
    }

    fun foreignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        verifier.verifyForeignKey(constraintName, srcColumn, targetTable, targetColumn)
    }

    fun foreignKey(constraintName: String? = null, expectedColumns: List<Pair<FullColumnName, FullColumnName>>) {
        verifier.verifyForeignKey(constraintName, expectedColumns)
    }

    fun primaryKey(constraintName: String?=null, expectedColumn: String) {
        primaryKey(constraintName, listOf(expectedColumn))
    }

    fun primaryKey(constraintName: String?=null, expectedColumns: List<String>) {
        verifier.verifyPrimaryKey(constraintName, expectedColumns)
    }

    fun unique(constraintName: String?=null, expectedColumn: String) {
        unique(constraintName, listOf(expectedColumn))
    }

    fun unique(constraintName: String?=null, expectedColumns: List<String>) {
        verifier.verifyUnique(constraintName, expectedColumns)
    }

    fun complete(skipCheckedException: Boolean = true) {
        val errors = verifier.verifyAll(skipCheckedException)

        if(errors.isNotBlank()) {
            throw AssertionError(errors)
        }
    }
}
