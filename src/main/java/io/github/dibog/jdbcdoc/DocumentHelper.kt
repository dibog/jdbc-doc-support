package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.writeLn
import org.springframework.jdbc.core.JdbcTemplate
import java.nio.file.Files
import java.nio.file.Paths

class DocumentHelper(jdbc: JdbcTemplate, catalog: String, private val schema: String) {
    private val extractor = DatabaseInspector(jdbc, catalog, schema)

    fun fetchTable(tableName: String): TableDBInfo? {
        val fullTableName = extractor.toFullTableName(tableName)
        return extractor.getTable(fullTableName)
    }

    fun table(tableName: String, snippedName: String? = null, action: DocTableSupport.()->Unit = {}) {
        val support = DocTableSupport(this, snippedName ?: tableName, tableName)
        support.action()
        support.complete()
    }

    fun schema(snippedName: String?=schema) {
        val docFolder = Paths.get("target/snippets-jdbcdoc").also { Files.createDirectories(it) }

        Files.newBufferedWriter(docFolder.resolve("$snippedName.plantuml")).use { writer ->
            writer.writeLn("""
                @startuml
                
                hide circle
                skinparam linetype ortho

                """.trimIndent()
            )

            extractor.getAllTables().forEachIndexed { index, tableInfo ->

                writer.writeLn("""
                    entity "${tableInfo.tableName}" as t$index { 
                    }
                    
                    """.trimIndent())
            }

            writer.writeLn("@enduml")
        }
    }
}

