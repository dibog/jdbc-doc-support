package io.github.dibog.jdbcdoc

import org.springframework.jdbc.core.JdbcTemplate

class DocumentHelper(jdbc: JdbcTemplate, catalog: String, private val schema: String, private val context: Context = Context()) {
    private val extractor = DatabaseInspector(jdbc, catalog, schema, context)

    fun fetchTable(tableName: String): TableDBInfo? {
        val fullTableName = extractor.toFullTableName(tableName)
        return extractor.getTable(fullTableName)
    }

    fun table(tableName: String, snippedName: String? = null, action: DocTableSupport.()->Unit = {}) {
        val support = DocTableSupport(this, extractor, snippedName ?: tableName, tableName, context)
        support.action()
        support.complete()
    }

    fun schema(snippedName: String?="schema-$schema") {
        SchemaDocumenter(extractor).createSchemaDiagram(snippedName?:"schema")
    }
}

