package io.github.dibog.jdbcdoc

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

class SchemaDocumenter(tables: List<TableDBInfo>) {
    private val docFolder = Paths.get("target/snippets-jdbcdoc").also { Files.createDirectories(it) }
    private val tableMap = tables.associateBy { it.tableName }

    fun createDocumentation(snippedName: String) {
        Files.newBufferedWriter(docFolder.resolve("$snippedName.plantuml"), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use { writer ->

            writer.writeLn("""
                @startuml
                
                hide circle
                skinparam linetype ortho
                
            """.trimIndent())

            val ids = tableMap.values.mapIndexed { index, tableDBInfo ->  tableDBInfo.tableName to index }.toMap()

            tableMap.values.forEachIndexed { index, table ->
                writer.writeLn("""entity "${table.tableName}" as t${ids[table.tableName]} {""")
                    table.primaryKey?.let { pk->
                        pk.columnNames.forEach { colName ->
                            val column = table.columns.first { it.name==colName }
                            val nullable = if (column.isNullable) "" else "*"
                            writer.writeLn( "$nullable ${column.name.column} (${table.getIndiciesShortcuts(colName)})" )
                        }
                    }
                writer.writeLn("--")

                table.getNonPKColumns().forEach { column ->
                    val nullable = if (column.isNullable) "" else "*"
                    writer.writeLn( "$nullable ${column.name.column} (${table.getIndiciesShortcuts(column.name)})" )
                }

                writer.writeLn("}")
            }

            tableMap.values.forEachIndexed { index, tableDBInfo ->
                tableDBInfo.foreignKeys.forEach { fk ->
                    val src = fk.srcTableName
                    val dest = fk.destTableName

                    val srcIndex = ids[src]
                    val destIndex = ids[dest]


                    if(srcIndex!=null && destIndex!=null) {
                        val stable = tableMap[src]!!
                        val nullable = stable.columns
                                .filter { it.name in fk.mapping.keys }
                                .any { it.isNullable }

                        val cardinality = if(nullable) "0..1" else "1"
                        writer.writeLn("""
                            t$srcIndex --> "$cardinality" t$destIndex : ${fk.constraintName.constraint}
                        """.trimIndent())
                    }
                }
            }

            writer.writeLn("""
                
                @enduml
            """.trimIndent())
        }
    }
}

