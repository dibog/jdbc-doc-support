package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.CheckConstraintChecker
import io.github.dibog.jdbcdoc.entities.ForeignKeyConstraintChecker
import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullTableName
import io.github.dibog.jdbcdoc.entities.PrimaryKeyConstraintChecker
import io.github.dibog.jdbcdoc.entities.TmpCheckConstraint
import io.github.dibog.jdbcdoc.entities.TmpColumnInfo
import io.github.dibog.jdbcdoc.entities.TmpForeignKeyConstraint
import io.github.dibog.jdbcdoc.entities.TmpPrimaryKeyConstraint
import io.github.dibog.jdbcdoc.entities.TmpUniqueConstraint
import io.github.dibog.jdbcdoc.entities.UniqueConstraintChecker

class DocTableSupport(private val helper: DocumentHelper, tableName: String) {

    val NULL = true
    val NOT_NULL = false

    private val tableInfo = helper.fetchTable(tableName) ?: throw IllegalArgumentException("Unknown table '$tableName'")
    private val unverified = mutableListOf<Any>()

    init {
        unverified.addAll( tableInfo.columns )

        val checkConstraints = tableInfo.constraints
                .filterIsInstance<TmpCheckConstraint>()
                .collectBy { it.fullConstraintName  }
                .map { (name, constraints)  ->
                    CheckConstraintChecker(name,constraints.toSet())
                }
        unverified.addAll( checkConstraints )

        val pkConstraints = tableInfo.constraints
                .filterIsInstance<TmpPrimaryKeyConstraint>()
                .collectWith { it.fullConstraintName to it.fullColumnName }
                .map { (name, column) ->
                    PrimaryKeyConstraintChecker(name, column.toSet())
                }
        unverified.addAll( pkConstraints )

        val uniqueConstraints = tableInfo.constraints
                .filterIsInstance<TmpUniqueConstraint>()
                .collectWith { it.fullConstraintName to it.fullColumnName}
                .map { (name, column) ->
                    UniqueConstraintChecker(name, column.toSet())
                }
        unverified.addAll( uniqueConstraints )

        val foreignKeyConstraints = tableInfo.constraints
                .filterIsInstance<TmpForeignKeyConstraint>()
                .collectBy { it.fullConstraintName }
                .map { (name, constraints) ->
                    ForeignKeyConstraintChecker(name, constraints.toSet())
                }
        unverified.addAll( foreignKeyConstraints )
    }

    private val errorMessage = mutableListOf<String>()
    init {
        require(tableInfo!=null) {
            "Could not find table '$tableName'"
        }
    }

    fun column(name: String, expectedDataType: String, expectedNullability: Boolean, action: DocColumnSupport.()->Unit = {}) {
        checkColumn(name, expectedDataType, expectedNullability)

        val support = DocColumnSupport(this, name)
        support.action()
        support.complete()
    }

    private fun checkColumn(name: String, expectedDataType: String, expectedNullability: Boolean) {
        val columnName = toFullColumnName(name)
        val columnInfo = unverified.filterIsInstance<TmpColumnInfo>().firstOrNull { it.columnName==columnName }

        if(columnInfo==null) {
            errorMessage.add("Column '$columnName' does not exist")
            return
        }

        val actualNullability = tableInfo.columnNullability(columnName)
        val actualDataType = tableInfo.columnDataType(columnName)

        var errorOccure = false
        if(actualNullability!=expectedNullability) {
            val expected = if(expectedNullability) "nullable" else "not nullable"
            val actual = if(actualNullability) "nullable" else "not nullable"
            errorMessage.add( "Column '$columnName' is expected to be $expected but is $actual")
            errorOccure = true
        }

        if(expectedDataType!=actualDataType) {
            errorMessage.add("Column '$columnName' is expected to be $expectedDataType but is $actualDataType")
            errorOccure = true
        }

        if(!errorOccure) {
            unverified.remove(columnInfo)
        }
    }

    private fun toFullTargetColumnName(tableName: String, columnName: String): FullColumnName {
        val (catalog, schema, table) = tableInfo.name
        return FullColumnName(catalog, schema, tableName.toUpperCase(), columnName.toUpperCase())
    }

    private fun toFullColumnName(columns: Set<String>): Set<FullColumnName> {
        return columns.map { toFullColumnName(it) }.toSet()
    }

    fun toFullColumnName(columnName: String): FullColumnName {
        val (catalog, schema, table) = tableInfo.name
        return FullColumnName(catalog, schema, table, columnName.toUpperCase())
    }

    fun foreignKey(constraintName: String? = null, srcColumns: List<String>, targetTable: String, targetColumns: List<String>) {
        require(srcColumns.size == targetColumns.size )
        val srcColumnNames = srcColumns.map { tableInfo.name.toFullColumnName(it) }
        val (catalog, schema, _) = tableInfo.name
        val destTableName = FullTableName( catalog, schema, targetTable.toUpperCase() )
        val destColumnNames = targetColumns.map { destTableName.toFullColumnName(it.toUpperCase()) }
        foreignKey(constraintName, srcColumnNames.zip(destColumnNames) )
    }

    fun foreignKey(constraintName: String? = null, expectedColumns: List<Pair<FullColumnName, FullColumnName>>) {
        val actualFks = unverified.filterIsInstance<ForeignKeyConstraintChecker>().filter { it.fullConstraintName==constraintName}
    }

    fun primaryKey(constraintName: String?=null, expectedColumn: String) {
        primaryKey(constraintName, setOf(expectedColumn))
    }

    fun primaryKey(constraintName: String?=null, expectedColumns: Set<String>) {
        val expectedColumns = toFullColumnName(expectedColumns)
        val actualPk  = unverified.filterIsInstance<PrimaryKeyConstraintChecker>().firstOrNull()
        when {
            actualPk==null && constraintName==null -> errorMessage.add("Expected $expectedColumns to be primary key, but table has no primary key")
            actualPk==null && constraintName!=null -> errorMessage.add("Expected $expectedColumns to be primary key '$constraintName', but table has no primary key")
            actualPk!=null -> {
                val actualColumns = actualPk.columns
                if(expectedColumns!=actualColumns) {
                    if(constraintName!=null) {
                        errorMessage.add("Expected $expectedColumns to be primary key '$constraintName', but $actualColumns are the primary key")
                    }
                    else {
                        errorMessage.add("Expected $expectedColumns to be primary key, but $actualColumns are the primary key")
                    }
                }
                else {
                    unverified.remove(actualPk)
                }
           }
        }
    }

    fun unique(name: String?=null, expectedColumn: String) {
        unique(name, setOf(expectedColumn))
    }

    fun unique(name: String?=null, expectedColumns: Set<String>) {
        val expectedColumns = toFullColumnName(expectedColumns)
        val actualUnique  = unverified.filterIsInstance<UniqueConstraintChecker>().firstOrNull()
        when {
            actualUnique==null && name==null -> errorMessage.add("Expected $expectedColumns to be unique")
            actualUnique==null && name!=null -> errorMessage.add("Expected $expectedColumns to be unique '$name'")
            actualUnique!=null -> {
                val actualColumns = actualUnique.columns
                if(expectedColumns!=actualColumns) {
                    if(name!=null) {
                        errorMessage.add("Expected $expectedColumns to be unique '$name', but $actualColumns were")
                    }
                    else {
                        errorMessage.add("Expected $expectedColumns to be unique")
                    }
                }
                else {
                    unverified.remove(actualUnique)
                }
            }
        }
    }

    fun complete(skipCheckedException: Boolean = true) {
        if(errorMessage.isNotEmpty()) {
            throw AssertionError(
                    errorMessage.joinToString("\n") { it }
            )
        }
        val unverified = if(skipCheckedException) {
            unverified.filter { it !is CheckConstraintChecker }
        }
        else {
            unverified
        }

        if(unverified.isNotEmpty()) {
            throw AssertionError(
                unverified.joinToString("\n") { it.toString() }
            )
        }
    }

}
