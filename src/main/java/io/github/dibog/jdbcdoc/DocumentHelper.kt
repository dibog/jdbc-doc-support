package io.github.dibog.jdbcdoc

import org.springframework.jdbc.core.JdbcTemplate

class DocumentHelper(jdbc: JdbcTemplate, catalog: String, schema: String) {
    private val extractor = Extractor(jdbc, catalog, schema)

    fun fetchTable(tableName: String): TableInfo? {
        val fullTableName = extractor.toFullTableName(tableName)
        return extractor.getTable(fullTableName)
    }
}

