package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import java.lang.IllegalStateException

class Extractor(private val jdbc: JdbcTemplate, catalog: String, schema: String) {
    private val tables : Map<FullTableName, TableInfo>
    var foreignKeys: Map<FullTableName, List<ForeignKeyConstraint>>
    var uniques: Map<FullTableName, List<UniqueConstraint>>
    var primaryKeys: Map<FullTableName, List<PrimaryKeyConstraint>>
    var checks: Map<FullTableName, List<CheckConstraint>>
    var columns: Map<FullTableName, List<ColumnInfo>>
    val catalog = catalog.toUpperCase()
    val schema = schema.toUpperCase()

    init {
        columns = loadAllColumns().groupBy { it.name.fullTableName }
        checks = loadAllCheckConstraints().groupBy { it.tableName }
        primaryKeys = loadAllPrimaryKeys().groupBy { it.tableName }
        uniques = loadAllUnique().groupBy { it.tableName }
        foreignKeys = loadAllForeignKeys().groupBy { it.srcTableName }

        val allTables = columns.keys + checks.keys + primaryKeys.keys + uniques.keys + foreignKeys.keys

        tables = allTables.map { tableName ->
            TableInfo(
                    tableName,
                    columns[tableName] ?: listOf(),
                    primaryKeys[tableName]?.firstOrNull(),
                    uniques[tableName] ?: listOf(),
                    checks[tableName] ?: listOf(),
                    foreignKeys[tableName] ?: listOf()
            )
        }.associateBy { it.tableName }
    }

    private fun loadAllColumns(): List<ColumnInfo> {
        return jdbc.query("""
            select *
            from information_schema.columns
            where table_catalog=? and table_schema=?
            """.trimIndent(),
                arrayOf(catalog.toUpperCase(), schema.toUpperCase())
        ) { rs, _ ->
            val fullColumnName = rs.extractFullColumnName()

            val dataType = rs.getString("DATA_TYPE")
            val isNullable = rs.getString("IS_NULLABLE")=="YES"
            val position = rs.getInt("ORDINAL_POSITION")

            ColumnInfo(fullColumnName, dataType, isNullable, position)
        }
    }

    private fun loadAllCheckConstraints(): List<CheckConstraint> {
        return jdbc.query("""
            select ccu.*, cc.CHECK_CLAUSE
            from information_schema.check_constraints cc, 
                 information_schema.constraint_column_usage ccu
            where ccu.table_catalog=? and ccu.table_schema=?
              and ccu.CONSTRAINT_CATALOG=cc.CONSTRAINT_CATALOG and ccu.CONSTRAINT_SCHEMA=cc.CONSTRAINT_SCHEMA and ccu.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
        """.trimIndent(),
                arrayOf(catalog.toUpperCase(), schema.toUpperCase()),
                ResultSetExtractor<List<CheckConstraint>> { rs ->
                    val result = mutableMapOf<FullConstraintName, CheckConstraint.Builder>()
                    while(rs.next()) {
                        val fullConstraintName = rs.extractFullConstraintName()
                        val fullColumnName = rs.extractFullColumnName()
                        val checkClause = rs.getString("CHECK_CLAUSE")

                        result.getOrPut(fullConstraintName,  { CheckConstraint.Builder(fullConstraintName, checkClause) })
                                .addColumnName(fullColumnName)
                    }

                    result.values.map { it.build() }
                }
        )
    }

    private fun loadAllPrimaryKeys() = loadAllKeyCandidates("PRIMARY KEY").map { (name,columns) ->
        PrimaryKeyConstraint(name, columns)
    }

    private fun loadAllUnique() = loadAllKeyCandidates("UNIQUE").map { (name,columns) ->
        UniqueConstraint(name, columns)
    }

    private fun loadAllKeyCandidates(type: String): List<KeyCandidateConstraint> {
        return jdbc.query("""
            select tc.*, ccu.column_name
            from information_schema.table_constraints tc,
                 information_schema.constraint_column_usage ccu
            where tc.table_catalog=? and tc.table_schema=?   
              and tc.CONSTRAINT_TYPE=?
              and tc.CONSTRAINT_CATALOG=ccu.CONSTRAINT_CATALOG
              and tc.CONSTRAINT_SCHEMA=ccu.CONSTRAINT_SCHEMA
              and tc.CONSTRAINT_NAME=ccu.CONSTRAINT_NAME
            """.trimIndent(),
                arrayOf(catalog.toUpperCase(), schema.toUpperCase(), type.toUpperCase()),
                ResultSetExtractor<List<KeyCandidateConstraint>> { rs ->
                    val result = mutableMapOf<FullConstraintName, KeyCandidateConstraint.Builder>()

                    while(rs.next()) {
                        val fullConstraintName = rs.extractFullConstraintName()
                        val fullColumnName = rs.extractFullColumnName()
                        result.getOrPut(fullConstraintName,  { KeyCandidateConstraint.Builder(fullConstraintName) })
                                .addColumnName(fullColumnName)
                    }

                    result.values.map { it.build() }
                })
    }

    private fun loadAllForeignKeys(): List<ForeignKeyConstraint> {
        val srcKeys = loadAllKeyCandidates("FOREIGN KEY").associateBy { it.constraintName  }
        val destKeys = (loadAllKeyCandidates("PRIMARY KEY")+
                loadAllKeyCandidates("UNIQUE")).associateBy { it.constraintName }

        return jdbc.query("""
            select * 
            from information_schema.REFERENTIAL_CONSTRAINTS
            where CONSTRAINT_CATALOG=?
              and CONSTRAINT_SCHEMA=?
            """.trimIndent(),
                arrayOf(catalog.toUpperCase(), schema.toUpperCase())
        ) { rs, _ ->
            val srcConstraintName = rs.extractFullConstraintName()
            val destConstraintName = rs.extractFullConstraintName("UNIQUE_")
            val src = srcKeys[srcConstraintName] ?: throw IllegalStateException("Unknown foreign key '$srcConstraintName'")
            val dest = destKeys[destConstraintName] ?: throw IllegalStateException("Unknown target key '$srcConstraintName'")

            ForeignKeyConstraint(srcConstraintName, src.columnNames, dest.columnNames)
        }
    }


    fun bringAllTogether() {
        val columns = loadAllColumns().groupBy { it.name.fullTableName }
        val checks = loadAllCheckConstraints().groupBy { it.tableName }
        val primaryKeys = loadAllPrimaryKeys().groupBy { it.tableName }
        val uniques = loadAllUnique().groupBy { it.tableName }
        val foreignKeys = loadAllForeignKeys().groupBy { it.srcTableName }

        val tables = columns.keys + checks.keys + primaryKeys.keys + uniques.keys + foreignKeys.keys

        tables.forEach { tableName ->
            TableInfo(
                    tableName,
                    columns[tableName] ?: listOf(),
                    primaryKeys[tableName]?.firstOrNull(),
                    uniques[tableName] ?: listOf(),
                    checks[tableName] ?: listOf(),
                    foreignKeys[tableName] ?: listOf()
            )
        }
    }

    fun getAllTables(): List<TableInfo> {
        return tables.values.toList()
    }

    fun toFullTableName(tableName: String): FullTableName {
        return FullTableName(catalog, schema, tableName.toUpperCase())
    }

    fun getTable(fullTableName: FullTableName): TableInfo? {
        return tables[fullTableName]
    }
}

class TableInfo(
        val tableName: FullTableName,
        val columns: List<ColumnInfo>,
        val primaryKey: PrimaryKeyConstraint?,
        val uniques: List<UniqueConstraint>,
        val checks: List<CheckConstraint>,
        val foreignKeys: List<ForeignKeyConstraint>
)

data class ForeignKeyConstraint(
        val constraintName: FullConstraintName,
        val srcColumnNames: List<FullColumnName>,
        val destColumnNames: List<FullColumnName>
) {
    constructor(srcConstraintName: FullConstraintName, srcColumnNames: FullColumnName, destColumnNames: FullColumnName)
        : this(srcConstraintName, listOf(srcColumnNames), listOf(destColumnNames))

    val srcTableName: FullTableName
        get() = srcColumnNames[0].fullTableName

    val destTableName: FullTableName
        get() = destColumnNames[0].fullTableName
}

data class KeyCandidateConstraint(
        val constraintName: FullConstraintName,
        val columnNames: MutableList<FullColumnName>
) {
    class Builder(private val constraintName: FullConstraintName) {
        private val columnNames = mutableListOf<FullColumnName>()
        fun addColumnName(columnName: FullColumnName) {
            columnNames.add(columnName)
        }
        fun build() = KeyCandidateConstraint(constraintName, columnNames)
    }
}

data class PrimaryKeyConstraint(
        val constraintName: FullConstraintName,
        val columnNames: List<FullColumnName>
)
{
    constructor(constraintName: FullConstraintName, columnNames: FullColumnName)
            : this(constraintName, listOf(columnNames))

    val tableName: FullTableName
        get() = columnNames[0].fullTableName
}

data class UniqueConstraint(
        val constraintName: FullConstraintName,
        val columnNames: List<FullColumnName>
) {
    constructor(constraintName: FullConstraintName, columnNames: FullColumnName)
            : this(constraintName, listOf(columnNames))

    val tableName: FullTableName
        get() = columnNames[0].fullTableName
}

data class CheckConstraint(
        val constraintName: FullConstraintName,
        val columnNames: List<FullColumnName>,
        val checkClause: String
) {
    class Builder(private val constraintName: FullConstraintName, private val checkClause: String) {
        private val columnNames = mutableListOf<FullColumnName>()
        fun addColumnName(columnName: FullColumnName) {
            columnNames.add(columnName)
        }
        fun build() = CheckConstraint(constraintName, columnNames, checkClause)
    }

    val tableName: FullTableName
        get() = columnNames[0].fullTableName
}

data class ColumnInfo(val name: FullColumnName, val dataType: String, val isNullable: Boolean, val position: Int)