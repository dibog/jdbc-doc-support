package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName

/** Column Info object as specified by the user. */
data class TableUserInfo(
        val tableName: FullTableName,
        val columnInfos: Map<FullColumnName, ColumnUserInfo>,
        val primarykey: PrimaryKeyConstraint?,
        val uniqueKeys: Map<FullConstraintName, UniqueConstraint>,
        val foreignKeys: Map<FullConstraintName, ForeignKeyConstraint>,
        val checkConstraints: Map<FullConstraintName, CheckConstraint>
) {
    private val shortCuts : Map<FullConstraintName, String>

    init {
        val builder = mutableMapOf<FullConstraintName, String>()
        primarykey?.let {
            builder[it.constraintName!!] = "PK"
        }
        uniqueKeys.keys.forEachIndexed { index, fullConstraintName ->  builder[fullConstraintName] = "UK$index" }
        foreignKeys.keys.forEachIndexed { index, fullConstraintName ->  builder[fullConstraintName] = "FK$index" }
        checkConstraints.keys.forEachIndexed { index, fullConstraintName ->  builder[fullConstraintName] = "CC$index" }

        shortCuts = builder
    }

    fun shortCuts(constraintName: FullConstraintName): String? {
        return shortCuts[constraintName]
    }
}

class TableUserInfoBuilder(private val table: TableDBInfo) {
    private val tableName = table.tableName
    private val columnInfos = mutableMapOf<FullColumnName,ColumnUserInfo>()
    private var primaryKey : PrimaryKeyConstraint? = null
    private val foreignKeys = mutableMapOf<FullConstraintName, ForeignKeyConstraint>()
    private val uniqueKeys = mutableMapOf<FullConstraintName, UniqueConstraint>()
    private val checkConstraints = mutableMapOf<FullConstraintName, CheckConstraint>()

    fun build(): TableUserInfo {
        return TableUserInfo(
                tableName,
                columnInfos,
                primaryKey,
                uniqueKeys,
                foreignKeys,
                checkConstraints)
    }

    fun addColumnInfo(columnName: String, dataType: UserDataType, nullability: Boolean, comment: String?) {
        val fullColumnName = tableName.toFullColumnName(columnName)
        columnInfos[fullColumnName] = ColumnUserInfo(fullColumnName, dataType, nullability, comment)
    }

    fun addCheckConstraint(constraintName: String, columns: List<String>, clause: String?) {
        val constraintName = tableName.toFullConstraintName(constraintName)
        val columns = columns.map { tableName.toFullColumnName(it) }

        val doc = CheckConstraint( constraintName, columns, clause )
        checkConstraints[constraintName] = doc
    }

    fun documentForeignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        val srcColumn = tableName.toFullColumnName(srcColumn)
        val targetTable = FullTableName(tableName.catalog, tableName.schema, targetTable)
        val destColumn = targetTable.toFullColumnName(targetColumn)

        documentForeignKey(constraintName, mapOf(srcColumn to destColumn))
    }

    fun documentForeignKey(constraintName: String?, columns: Map<FullColumnName, FullColumnName>) {
        val constraintName = if(constraintName==null) {
            table.foreignKeys.filter { it.mapping==columns }.map { it.constraintName }.first()
        }
        else {
            tableName.toFullConstraintName(constraintName)
        }

        foreignKeys[constraintName] = ForeignKeyConstraint( constraintName, columns )
    }

    fun documentPrimaryKey(constraintName: String?, columns: List<String>) {
        val columns = columns.map { tableName.toFullColumnName(it) }
        val constraintName = if(constraintName==null) {
            require(table.primaryKey!!.columnNames==columns)
            table.primaryKey!!.constraintName
        } else {
            tableName.toFullConstraintName(constraintName)
        }

        primaryKey = PrimaryKeyConstraint(constraintName, columns)
    }

    fun documentUniqueKey(constraintName: String?, columns: List<String>) {
        val columns = columns.map { tableName.toFullColumnName(it) }
        val constraintName = if(constraintName==null) {
            table.uniques.first { it.columnNames == columns }.constraintName
        }
        else {
            tableName.toFullConstraintName(constraintName)
        }

        uniqueKeys[constraintName] = UniqueConstraint(constraintName, columns)
    }
}

/** Column Info object as specified by the user. */
data class ColumnUserInfo(val name: FullColumnName, val dataType: UserDataType, val nullability: Boolean, val comment: String?)


sealed class UserDataType {
    data class GenericDataType(val type: String) : UserDataType() {
        override fun toString() = type
    }
    data class CharacterVarying(val maxLength: Int) : UserDataType() {
        override fun toString() = "character varying($maxLength)"
    }
}