package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullTableName
import io.github.dibog.jdbcdoc.entities.TableInfo
import org.springframework.jdbc.core.JdbcTemplate

class DocumentHelper(jdbc: JdbcTemplate, catalog: String, schema: String) {
    val catalog = catalog.toUpperCase()
    val schema = schema.toUpperCase()

    private val checkConstraints = jdbc.fetchAllCheckConstraintsOf(catalog, schema)
    private val primaryKeyConstraint = jdbc.fetchAllPrimaryKeyConstraintsOf(catalog, schema)
    private val uniqueConstraint = jdbc.fetchAllUniqueConstraintsOf(catalog, schema)
    private val foreignKeyConstraint = jdbc.fetchAllForeignKeyConstraintsFor(catalog, schema)
    private val columnInfo = jdbc.fetchAllColumnInfosFor(catalog, schema)

    fun allTables(): List<TableInfo> {
        val allTables = checkConstraints.map { it.fullColumnName.fullTableName }.toSet() +
                primaryKeyConstraint.map { it.fullColumnName.fullTableName }.toSet() +
                uniqueConstraint.map { it.fullColumnName.fullTableName }.toSet() +
                foreignKeyConstraint.map { it.fullSrcColumnName.fullTableName }.toSet()

        return allTables.mapNotNull { fetchTable( it.table ) }
    }

    fun fetchTable(tableName: String): TableInfo? {
        val tableName = FullTableName(catalog, schema, tableName.toUpperCase())
        val tableCC = checkConstraints.filter { it.fullColumnName.fullTableName==tableName }
        val tablePK = primaryKeyConstraint.filter { it.fullColumnName.fullTableName==tableName }
        val tableUQ = uniqueConstraint.filter { it.fullColumnName.fullTableName==tableName }
        val tableFK = foreignKeyConstraint.filter { it.fullSrcColumnName.fullTableName==tableName }
        val constraints = tableCC + tablePK + tableUQ + tableFK

        val columns = columnInfo.filter { it.columnName.fullTableName==tableName }
        return if(constraints==null) null else TableInfo(tableName, columns, constraints)
    }
}

