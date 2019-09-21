package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor

class DatabaseInspector(private val jdbc: JdbcTemplate, catalog: String, schema: String) {
    private val tables : Map<FullTableName, TableDBInfo>
    private val dialect : SqlDialect = SqlDialect.createDialect(jdbc.dataSource.connection)
    private val catalog = dialect.catalog(catalog)
    private val schema = dialect.schema(schema)

    var foreignKeys: Map<FullTableName, List<ForeignKeyConstraint>>
    var uniques: Map<FullTableName, List<UniqueConstraint>>
    var primaryKeys: Map<FullTableName, List<PrimaryKeyConstraint>>
    var checks: Map<FullTableName, List<CheckConstraint>>
    var columns: Map<FullTableName, List<ColumnDBInfo>>

    init {
        val keyColumnUsage = loadKeyColumnUsage()
        columns = loadAllColumns().groupBy { it.name.fullTableName }
        checks = loadAllCheckConstraints().groupBy { it.tableName }
        primaryKeys = loadAllPrimaryKeys(keyColumnUsage).groupBy { it.tableName }
        uniques = loadAllUnique(keyColumnUsage).groupBy { it.tableName }
        foreignKeys = loadAllForeignKeys(keyColumnUsage).groupBy { it.srcTableName }

        val allTables = columns.keys + checks.keys + primaryKeys.keys + uniques.keys + foreignKeys.keys

        tables = allTables.map { tableName ->
            TableDBInfo(
                    tableName,
                    columns[tableName] ?: listOf(),
                    primaryKeys[tableName]?.firstOrNull(),
                    uniques[tableName] ?: listOf(),
                    checks[tableName] ?: listOf(),
                    foreignKeys[tableName] ?: listOf()
            )
        }.associateBy { it.tableName }
    }

    private fun loadKeyColumnUsage(): Map<FullConstraintName,KeyColumnUsage> {
        return jdbc.query("""
            select * 
             from information_schema.key_column_usage
            where constraint_catalog=?
              and constraint_schema=?
              order by constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name, ordinal_position
        """.trimIndent(),
                arrayOf(catalog, schema),
                ResultSetExtractor<Map<FullConstraintName,KeyColumnUsage>> { rs ->
                    val result = mutableMapOf<FullConstraintName, KeyColumnUsage>()
                    val columns = mutableListOf<FullColumnName>()
                    val index = mutableListOf<Int>()

                    var lastConstraintName : FullConstraintName? = null

                    while(rs.next()) {
                        val fullConstraintName = rs.extractFullConstraintName()
                        val fullColumnName = rs.extractFullColumnName()
                        val positionInUniqueConstraint = rs.getString("POSITION_IN_UNIQUE_CONSTRAINT")?.toIntOrNull()

                        if(fullConstraintName!=lastConstraintName) {
                            if(lastConstraintName!=null) {
                                result[lastConstraintName] = KeyColumnUsage(ArrayList(columns), if(index.isEmpty()) null else ArrayList(index))
                            }
                            columns.clear()
                            index.clear()
                            lastConstraintName=fullConstraintName
                        }

                        columns.add(fullColumnName)
                        if(positionInUniqueConstraint!=null) {
                            index.add(positionInUniqueConstraint-1) // as SQL returns index starting from 1
                        }
                    }

                    if(lastConstraintName!=null) {
                        result[lastConstraintName] = KeyColumnUsage(ArrayList(columns), if (index.isEmpty()) null else ArrayList(index))
                    }

                    result
                }
        )
    }

    private fun loadAllColumns(): List<ColumnDBInfo> {
        return jdbc.query("""
            select *
            from information_schema.columns
            where table_catalog=? and table_schema=?
            """.trimIndent(),
                arrayOf(catalog, schema)
        ) { rs, _ ->
            val fullColumnName = rs.extractFullColumnName()

            val dataType = rs.getString("DATA_TYPE")
            val isNullable = rs.getString("IS_NULLABLE")=="YES"
            val position = rs.getInt("ORDINAL_POSITION")

            ColumnDBInfo(fullColumnName, dataType, isNullable, position)
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
                arrayOf(catalog, schema),
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

    private fun loadAllPrimaryKeys(indices: Map<FullConstraintName,KeyColumnUsage>) = loadAllKeyCandidates("PRIMARY KEY", indices).map { (name,columns) ->
        PrimaryKeyConstraint(name, columns)
    }

    private fun loadAllPrimaryKeys() = loadAllKeyCandidates("PRIMARY KEY").map { (name,columns) ->
        PrimaryKeyConstraint(name, columns)
    }

    private fun loadAllUnique(indices: Map<FullConstraintName,KeyColumnUsage>) = loadAllKeyCandidates("UNIQUE", indices).map { (name,columns) ->
        UniqueConstraint(name, columns)
    }

    private fun loadAllUnique() = loadAllKeyCandidates("UNIQUE").map { (name,columns) ->
        UniqueConstraint(name, columns)
    }

    private fun loadAllKeyCandidates(type: String, indices: Map<FullConstraintName,KeyColumnUsage>): List<KeyCandidateConstraint> {
        return jdbc.query("""
            select *
              from information_schema.table_constraints
             where table_catalog=? 
               and table_schema=? 
               and constraint_type=?
            """.trimIndent(),
                arrayOf(catalog, schema, type),
                ResultSetExtractor<List<KeyCandidateConstraint>> { rs ->
                    val result = mutableListOf<KeyCandidateConstraint>()

                    while(rs.next()) {
                        val fullConstraintName = rs.extractFullConstraintName()
                        val keyColumnUsage = indices[fullConstraintName] ?: throw IllegalStateException("Unknown constraint name '$fullConstraintName'")
                        result.add(KeyCandidateConstraint(fullConstraintName, keyColumnUsage.columns))
                    }

                    result
                })
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
                arrayOf(catalog, schema, type),
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

    private fun loadAllForeignKeys(indices: Map<FullConstraintName,KeyColumnUsage>): List<ForeignKeyConstraint> {
        return jdbc.query("""
            select * 
            from information_schema.REFERENTIAL_CONSTRAINTS
            where CONSTRAINT_CATALOG=?
              and CONSTRAINT_SCHEMA=?
            """.trimIndent(),
                arrayOf(catalog, schema)
        ) { rs, _ ->
            val srcConstraintName = rs.extractFullConstraintName()
            val destConstraintName = rs.extractFullConstraintName("UNIQUE_")
            val src = indices[srcConstraintName] ?: throw IllegalStateException("Unknown foreign key '$srcConstraintName'")
            val dest = indices[destConstraintName] ?: throw IllegalStateException("Unknown target key '$srcConstraintName'")

            ForeignKeyConstraint(srcConstraintName, src.indexMap(dest.columns))
        }
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
                arrayOf(catalog, schema)
        ) { rs, _ ->
            val srcConstraintName = rs.extractFullConstraintName()
            val destConstraintName = rs.extractFullConstraintName("UNIQUE_")
            val src = srcKeys[srcConstraintName] ?: throw IllegalStateException("Unknown foreign key '$srcConstraintName'")
            val dest = destKeys[destConstraintName] ?: throw IllegalStateException("Unknown target key '$srcConstraintName'")

            ForeignKeyConstraint(srcConstraintName, src.columnNames.zip(dest.columnNames).toMap())
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
            TableDBInfo(
                    tableName,
                    columns[tableName] ?: listOf(),
                    primaryKeys[tableName]?.firstOrNull(),
                    uniques[tableName] ?: listOf(),
                    checks[tableName] ?: listOf(),
                    foreignKeys[tableName] ?: listOf()
            )
        }
    }

    fun getAllTables(): List<TableDBInfo> {
        return tables.values.toList()
    }

    fun toFullTableName(tableName: String): FullTableName {
        return FullTableName(catalog, schema, dialect.table(tableName))
    }

    fun getTable(fullTableName: FullTableName): TableDBInfo? {
        return tables[fullTableName]
    }

    fun toFullConstraintName(constraint: String): FullConstraintName {
        return FullConstraintName(catalog, schema, dialect.constraint(constraint))
    }

    fun toFullColumnName(table: String, column: String): FullColumnName {
        return FullColumnName(catalog, schema, dialect.table(table), dialect.column(column))
    }

    fun toFullColumnNames(srcTable: String, srcColumns: List<String>, destTable: String, destColumns: List<String>): Map<FullColumnName,FullColumnName> {
        require(srcColumns.size==destColumns.size)
        return srcColumns.zip(destColumns).associate { (srcColumn, destColumn) ->
            toFullColumnName(srcTable, srcColumn) to toFullColumnName(destTable, destColumn)
        }
    }
}

data class ForeignKeyConstraint(
        val constraintName: FullConstraintName,
        val mapping: Map<FullColumnName, FullColumnName>
) {
//    companion object {
//        fun asMap(src: List<FullColumnName>, dest: List<FullColumnName>): Map<FullColumnName,FullColumnName> {
//            require(src.size==dest.size)
//            return src.zip(dest).associate { (src,dest) -> src to test  }
//        }
//    }
    constructor(srcConstraintName: FullConstraintName, srcColumnName: FullColumnName, destColumnName: FullColumnName)
        : this(srcConstraintName, mapOf(srcColumnName to destColumnName))

    val srcTableName: FullTableName
        get() = mapping.iterator().next().key.fullTableName

    val destTableName: FullTableName
        get() = mapping.iterator().next().value.fullTableName
}

data class KeyCandidateConstraint(
        val constraintName: FullConstraintName,
        val columnNames: List<FullColumnName>
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

data class KeyColumnUsage(val columns: List<FullColumnName>, val targetIndex: List<Int>?) {
    init {
        require(targetIndex==null || columns.size==targetIndex.size)
    }

    fun indexMap(targetKeys: List<FullColumnName>): Map<FullColumnName,FullColumnName> {
        require(targetIndex!=null && targetIndex.size==targetKeys.size)

        return columns.mapIndexed { index, fullColumnName ->
            fullColumnName to targetKeys[targetIndex[index]]
        }.toMap()
    }
}
