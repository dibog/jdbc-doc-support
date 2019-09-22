package io.github.dibog.jdbcdoc

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

class TableDocumenter(private val dbTable: TableDBInfo, private val userTable: TableUserInfo) {
    private val docFolder = Paths.get("target/snippets-jdbcdoc").also { Files.createDirectories(it) }

    fun createDocumentation(snippedName: String) {
        val indexMap = userTable.columnInfos.map { (name, colInfo) ->
            val list= mutableListOf<String>()

            userTable.primarykey?.let {
                if(name in it.columnNames) {
                    list.add(userTable.shortCuts(it.constraintName)!!)
                }
            }

            userTable.uniqueKeys
                    .filter { name in it.value.columnNames }
                    .map { userTable.shortCuts(it.key)!! }
                    .forEach { list.add(it) }

            userTable.foreignKeys
                    .filter { name in it.value.mapping.keys }
                    .map { userTable.shortCuts(it.key)!! }
                    .forEach { list.add(it) }

            userTable.checkConstraints
                    .filter { name in it.value.columnNames }
                    .map { userTable.shortCuts(it.key)!! }
                    .forEach { list.add(it) }

            name to list.joinToString(",") { it }
        }.toMap()

        Files.newBufferedWriter(docFolder.resolve("$snippedName.adoc"), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use {
            writer  ->

            writer.writeLn(".Table ${userTable.tableName}")
            writer.writeLn("|===")
            writer.writeLn("| Indices | Column Name | Data Type | Nullability | Comments")

            userTable.columnInfos.map { (name, colInfo) ->
                val indices = indexMap[name] ?: ""
                writer.writeLn("| $indices")
                writer.writeLn("| ${name.column}")
                writer.writeLn("| ${colInfo.dataType}")
                val nullable = if(colInfo.nullability) "NULL" else "NOT NULL"
                writer.writeLn("| $nullable")
                writer.writeLn("| ${colInfo.comment ?: ""}")
                writer.writeLn()
            }

            writer.writeLn("|===")

            userTable.primarykey?.let {
                writer.writeLn("${userTable.shortCuts(it.constraintName)}:: ${it.constraintName}")
            }
            dbTable.uniques.forEach {
                writer.writeLn("${userTable.shortCuts(it.constraintName)}:: ${it.constraintName}")
            }
            dbTable.foreignKeys.forEach {
                writer.writeLn("${userTable.shortCuts(it.constraintName)}:: ${it.constraintName}")
            }
            userTable.checkConstraints.forEach { it ->
                writer.writeLn("${userTable.shortCuts(it.key)}:: ${it.key}")
            }
        }
    }
}
