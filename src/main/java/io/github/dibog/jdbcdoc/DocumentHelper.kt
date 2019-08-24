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
        val allTables = checkConstraints.flatMap { it.fullColumnNames }.map { it.fullTableName }.toSet() +
                primaryKeyConstraint.flatMap { it.fullColumnNames }.map { it.fullTableName }.toSet() +
                uniqueConstraint.flatMap { it.fullColumnNames }.map { it.fullTableName }.toSet() +
                foreignKeyConstraint.flatMap { it.srcColumns }.map { it.fullTableName }.toSet()

        return allTables.mapNotNull { fetchTable( it.table ) }
    }

    fun fetchTable(tableName: String): TableInfo? {
        val tableName = FullTableName(catalog, schema, tableName.toUpperCase())
        val tableCC = checkConstraints.filter { it.fullColumnNames.any { it.fullTableName==tableName } }
        val tablePK = primaryKeyConstraint.filter { it.fullColumnNames.any { it.fullTableName==tableName } }
        val tableUQ = uniqueConstraint.filter { it.fullColumnNames.any { it.fullTableName==tableName } }
        val tableFK = foreignKeyConstraint.filter { it.srcColumns.any { it.fullTableName==tableName } }
        val constraints = tableCC + tablePK + tableUQ + tableFK

        val columns = columnInfo.filter { it.fullColumnName.fullTableName==tableName }
        return if(constraints==null) null else TableInfo(tableName, columns, constraints)
    }
}

