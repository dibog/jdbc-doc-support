package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullTableName
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

class SchemaDocumenter(private val inspector: DatabaseInspector) {
    private val docFolder = Paths.get("target/snippets-jdbcdoc").also { Files.createDirectories(it) }
    private val tableMap = inspector.getAllTables().associateBy { it.tableName }
    private val ids = tableMap.values.mapIndexed { index, tableDBInfo ->  tableDBInfo.tableName to index }.toMap()

    fun createSchemaDiagram(snippedName: String) {
        Files.newBufferedWriter(docFolder.resolve("$snippedName.plantuml"), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use { writer ->

            writer.writeLn("""
                hide circle
                skinparam linetype ortho
                
            """.trimIndent())

            tableMap.values.forEach { table ->
                writer.writeLn( tableFragment(table) )
            }

            tableMap.values.forEachIndexed { index, tableDBInfo ->
                tableDBInfo.foreignKeys.forEach { fk ->
                    foreignKeyFragment(fk)?.let {
                        writer.writeLn(it)
                    }
                }
            }
        }
    }

    fun createTableDiagram(tableName: FullTableName, snippedName: String=tableName.table) {

        val incoming = inspector.getIncomingRelations(tableName)
        val incomingTable = incoming.map { it.srcTableName }.toSet()

        val outgoing = inspector.getOutgoingRelations(tableName)
        val outgoingTable = outgoing.map { it.destTableName }.toSet()

        val tables = incomingTable + tableName + outgoingTable

        Files.newBufferedWriter(docFolder.resolve("$snippedName.plantuml"), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use { writer ->

            writer.writeLn("""
                hide circle
                skinparam linetype ortho
                
            """.trimIndent())

            tables.forEach { tableName ->
                val tableInfo = inspector.getTable(tableName)
                if(tableInfo!=null) {
                    writer.writeLn(tableFragment(tableInfo))
                }
            }

            incoming.forEach { fk ->
                foreignKeyFragment(fk)?.let {
                    writer.writeLn(it)
                }
            }

            outgoing.forEach { fk ->
                foreignKeyFragment(fk)?.let {
                    writer.writeLn(it)
                }
            }
        }
    }

    private fun foreignKeyFragment(fk: ForeignKeyConstraint): String? {
        val src = fk.srcTableName
        val dest = fk.destTableName

        val srcIndex = ids[src]
        val destIndex = ids[dest]

        return if(srcIndex!=null && destIndex!=null) {
            val stable = tableMap[src]!!
            val nullable = stable.columns
                    .filter { it.name in fk.mapping.keys }
                    .any { it.isNullable }
            val cardinality = if(nullable) "0..1" else "1"

            """t$srcIndex --> "$cardinality" t$destIndex : ${fk.constraintName.constraint}"""
        }
        else {
            null
        }
    }

    private fun tableFragment(table: TableDBInfo): String {
        return buildString {
            append("""
                entity "${table.tableName.table}" as t${ids[table.tableName]} {
                
            """.trimIndent())
            table.primaryKey?.let { pk ->
                pk.columnNames.forEach { colName ->
                    val column = table.columns.first { it.name == colName }
                    val nullable = if (column.isNullable) "" else "*"
                    append("$nullable ${column.name.column} (${table.getIndiciesShortcuts(colName)})\n")
                }
            }
            append("--\n")

            table.getNonPKColumns().forEach { column ->
                val nullable = if (column.isNullable) "" else "*"
                append("$nullable ${column.name.column} (${table.getIndiciesShortcuts(column.name)})\n")
            }

            append("}\n")
        }
    }
}

