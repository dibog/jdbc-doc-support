package io.github.dibog.jdbcdoc.entities

import io.github.dibog.jdbcdoc.CheckConstraint
import io.github.dibog.jdbcdoc.ColumnDBInfo
import io.github.dibog.jdbcdoc.ForeignKeyConstraint
import io.github.dibog.jdbcdoc.PrimaryKeyConstraint
import io.github.dibog.jdbcdoc.UniqueConstraint
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

class TableVerifier(
        private val tableName: FullTableName,
        private val pkVerifier : PrimaryKeyVerifier?,
        columnVerifiers : Iterable<ColumnVerifier>,
        uniqueVerifiers : Iterable<UniqueVerifier>,
        checkVerifiers : Iterable<CheckVerifier>,
        foreignKeyVerifiers : Iterable<ForeignKeyVerifier>
) {
    private val docFolder = Paths.get("target/snippets-jdbcdoc").also { Files.createDirectories(it) }

    private val columnVerifier = columnVerifiers.associateBy { it.columnName }
    private val uniqueVerifier = uniqueVerifiers.associateBy { it.constraintName }
    private val checkVerifier = checkVerifiers.associateBy { it.constraintName }
    private val foreignKeyVerifier = foreignKeyVerifiers.associateBy { it.constraintName }
    private val errorMessages = mutableListOf<String>()

    private var primaryKeyDoc: PrimaryKeyDoc? = null
    private val columnDocs = mutableMapOf<FullColumnName,ColumnDoc>()
    private val uniqueDocs = mutableListOf<UniqueDoc>()
    private val foreignKeyDocs = mutableListOf<ForeignKeyDoc>()
    private val checkConstraints = mutableMapOf<FullConstraintName, CheckConstraintDoc>()

    fun documentColumn(columnName: String, expectedDataType: String, expectedNullability: Boolean, comment: String?) {
        val columnName = tableName.toFullColumnName(columnName)
        val doc = ColumnDoc(columnName, expectedDataType, expectedNullability, comment)
        columnDocs[columnName] = doc
    }

    fun documentColumnComment(columnName: String, comment: String?) {
        val columnName = tableName.toFullColumnName(columnName)
        val doc = columnDocs[columnName]
        if(doc!=null) {
            columnDocs[columnName] = doc.copy(comment = comment)
        }
    }

    private fun verifyColumnDoc(doc: ColumnDoc) {
        val verifier = columnVerifier[doc.columnName]

        if (verifier==null) {
            errorMessages.add("Column '${doc.columnName}' has no verifier (perhaps it does not exist)")
        }
        else {
            verifier.verify(doc).addTo(errorMessages)
        }
    }

    fun documentPrimaryKey(constraintName: String?, expectedColumns: List<String>) {
        val expectedColumns = expectedColumns.map { tableName.toFullColumnName(it) }
        val constraintName = if(constraintName==null) {
            require(pkVerifier!!.columns==expectedColumns)
            pkVerifier!!.constraintName
        } else {
            tableName.toFullConstraintName(constraintName)
        }

        primaryKeyDoc = PrimaryKeyDoc(constraintName, expectedColumns)
    }

    private fun verifyPrimaryKeyDoc(doc: PrimaryKeyDoc?) {
        if(doc==null) return
        val value = pkVerifier

        if(pkVerifier==null) {
            if (doc.constraintName == null) {
                errorMessages.add("Table '$tableName' is expected to have a primary key, but actual has none")
            } else {
                errorMessages.add("Table '$tableName' is expected to have a primary key '${doc.constraintName}', but actual has none")
            }
        }
        else {
            pkVerifier.verify(doc).addTo(errorMessages)
        }
    }

    fun documentUnique(constraintName: String?, expectedColumns: List<String>) {
        val expectedColumns = expectedColumns.map { tableName.toFullColumnName(it) }
        val constraintName = if(constraintName==null) {
            uniqueVerifier.filter { it.value.columns==expectedColumns }.map { it.value }.first().constraintName
        }
        else {
            tableName.toFullConstraintName(constraintName)
        }

        val doc = UniqueDoc(constraintName, expectedColumns)
        uniqueDocs.add(doc)
    }

    private fun verifyUniqueDoc(doc: UniqueDoc) {
        val verifier = if(doc.constraintName==null) {
            val candidates = uniqueVerifier.filter { (name, uv) -> uv.columns==doc.expectedColumns }.map { (name, uv) -> uv}
            when {
                candidates.isEmpty() -> {
                    errorMessages.add("Could not find any unique constraints for '${doc.expectedColumns}'")
                    null
                }
                candidates.size==1 -> candidates[0]
                else -> {
                    errorMessages.add("Found more then one unique constraints for '${doc.expectedColumns}")
                    null
                }
            }
        }
        else {
            val verifier = uniqueVerifier[doc.constraintName]
            if(verifier==null) {
                errorMessages.add("Could not find any unique constraint with name '${doc.constraintName}")
                null
            }
            else {
                verifier
            }
        }

        verifier?.verify(doc)?.addTo(errorMessages)
    }

    fun documentForeignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        val expectedSrcColumn = tableName.toFullColumnName(srcColumn)
        val targetTable = FullTableName(tableName.catalog, tableName.schema, targetTable.toUpperCase())
        val expectedDestColumn = targetTable.toFullColumnName(targetColumn)

        documentForeignKey(constraintName, mapOf(expectedSrcColumn to expectedDestColumn))
    }

    fun documentForeignKey(constraintName: String?, expectedMapping: Map<FullColumnName, FullColumnName>) {

        val constraintName = if(constraintName==null) {
            foreignKeyVerifier.filter { it.value.actualMapping==expectedMapping }.map { it.key }.first()
        }
        else {
            tableName.toFullConstraintName(constraintName)
        }
        val doc = ForeignKeyDoc( constraintName, expectedMapping )
        foreignKeyDocs.add(doc)
    }

    private fun verifyForeignKeyDoc(doc: ForeignKeyDoc) {
        val verifier = if(doc.constraintName==null) {
            val candidates = foreignKeyVerifier.filter { (name,verifier) ->
                verifier.actualMapping==doc.expectedMapping
            }.map { (name, verifier) -> verifier}

            when {
                candidates.isEmpty() -> {
                    errorMessages.add("Could not find any foreign key constraints for '${doc.expectedMapping}'")
                    null
                }
                candidates.size==1 -> candidates[0]
                else -> {
                    errorMessages.add("Found more then one foreign key constraints for '${doc.expectedMapping}")
                    null
                }
            }
        }
        else {
            val fkVerifier = foreignKeyVerifier[doc.constraintName]
            if(fkVerifier==null) {
                errorMessages.add("Could not find any foreign key constraint '${doc.constraintName}'")
                null
            }
            else {
                fkVerifier
            }
        }

        verifier?.verify(doc)?.addTo(errorMessages)
    }

    fun documentCheckConstraint(constraintName: String, columns: List<String>, clause: String?) {
        val constraintName = tableName.toFullConstraintName(constraintName)
        val columns = columns.map { tableName.toFullColumnName(it) }

        val doc = CheckConstraintDoc( constraintName, columns, clause )
        checkConstraints[constraintName] = doc
    }

    private fun verifyCheckConstraintDoc(doc: CheckConstraintDoc) {
        val verifier = checkVerifier[doc.constraintName]
        if(verifier==null) {
            errorMessages.add("Could not find any check constraint '${doc.constraintName}'")
        }
        else {
            verifier.verify(doc).addTo(errorMessages)
        }
    }

    internal fun documentAll() {
        verifyPrimaryKeyDoc(primaryKeyDoc)

        uniqueDocs.forEach { doc->
            verifyUniqueDoc(doc)
        }

        foreignKeyDocs.forEach { doc ->
            verifyForeignKeyDoc(doc)
        }

        columnDocs.forEach { (_, doc) ->
            verifyColumnDoc(doc)
        }

        checkConstraints.forEach { (_, doc) ->
            verifyCheckConstraintDoc(doc)
        }
    }

    internal fun verifyAll(skipCheckedException: Boolean): String {
        if(pkVerifier!=null && !pkVerifier.verified) {
            errorMessages.add("Primary key '${pkVerifier.constraintName}' was not documented")
        }

        uniqueVerifier.forEach { (name, uv) ->
            if (!uv.verified) {
                errorMessages.add("Unique key constraint '${uv.constraintName}' was not documented")
            }
        }

        if(!skipCheckedException) {
            checkVerifier.forEach { (name, cv) ->
                if (!cv.verified) {
                    errorMessages.add("Check constraint '${cv.constraintName}' was not documented")
                }
            }
        }

        foreignKeyVerifier.forEach { (name, fv) ->
            if (!fv.verified) {
                errorMessages.add("Foreign key constraint '${fv.constraintName}' was not documented")
            }
        }

        columnVerifier.forEach { (name, cv) ->
            if (!cv.verified) {
                errorMessages.add("Column '${cv.columnName}' was not documented")
            }
        }

        return errorMessages.joinToString("\n") { it }
    }

    fun createDocumentation(snippedName: String) {
        val indexMap = columnDocs.map { (name,doc) ->
            val list= mutableListOf<String>()
            primaryKeyDoc?.let {
                if(it.expectedColumns.contains(name)) list.add("PK")
            }
            uniqueDocs.forEachIndexed { index, uniqueDoc ->
                if(uniqueDoc.expectedColumns.contains(name)) list.add("UC$index")
            }
            foreignKeyDocs.forEachIndexed { index, foreignKeyDoc ->
                if(foreignKeyDoc.expectedMapping.keys.contains(name)) list.add("FK$index")
            }
            checkConstraints.values.forEachIndexed { index, checkDoc ->
                if(checkDoc.columns.contains(name)) list.add("CC$index")
            }
            name to list.joinToString(",") { it }
        }.toMap()

        Files.newBufferedWriter(docFolder.resolve("$snippedName.adoc"), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).use {
            writer  ->

            writer.writeLn(".Table $tableName")
            writer.writeLn("|===")
            writer.writeLn("| Indices | Column Name | Data Type | Nullability | Comments")

            columnDocs.map { (name, doc) ->
                val indices = indexMap[name] ?: ""
                writer.writeLn("| $indices")
                writer.writeLn("| ${name.column}")
                writer.writeLn("| ${doc.expectedDataType}")
                val nullable = if(doc.expectedNullability) "NULL" else "NOT NULL"
                writer.writeLn("| $nullable")
                writer.writeLn("| ${doc.comment ?: ""}")
                writer.writeLn()
            }

            writer.writeLn("|===")

            primaryKeyDoc?.let {
                writer.writeLn("PK:: ${it.constraintName}")
            }
            uniqueDocs.forEachIndexed { index, doc ->
                writer.writeLn("UC$index:: ${doc.constraintName}")
            }
            foreignKeyDocs.forEachIndexed { index, foreignKeyDoc ->
                writer.writeLn("FK$index:: ${foreignKeyDoc.constraintName}")
            }
            checkConstraints.values.forEachIndexed { index, checkDoc ->
                writer.writeLn("CC$index:: ${checkDoc.constraintName}")
            }
        }
    }
}

fun BufferedWriter.writeLn(text: String = "") {
    write(text)
    newLine()
}

data class CheckConstraintDoc(val constraintName: FullConstraintName, val columns: List<FullColumnName>, val clause: String?)

data class ForeignKeyDoc(val constraintName: FullConstraintName?, val expectedMapping: Map<FullColumnName,FullColumnName>)

data class PrimaryKeyDoc(val constraintName: FullConstraintName?, val expectedColumns: List<FullColumnName>)

data class UniqueDoc(val constraintName: FullConstraintName?, val expectedColumns: List<FullColumnName>)

data class ColumnDoc(val columnName: FullColumnName, val expectedDataType: String, val expectedNullability: Boolean, val comment: String?)

open class Verifier {
    var verified = false
        protected set(value) { field = value }
}

open class ConstraintVerifier(val constraintName: FullConstraintName) : Verifier()

class PrimaryKeyVerifier( constraintName: FullConstraintName, val columns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    constructor(pk: PrimaryKeyConstraint) : this(pk.constraintName, pk.columnNames)

    fun verify(doc: PrimaryKeyDoc?): List<String> {
        if(doc==null) return listOf()
        val errors = mutableListOf<String>()

        if(doc.expectedColumns!=columns) {
            if(constraintName!=null) {
                errors.add("Expected '${doc.expectedColumns}' to be primary key '$constraintName', but '$columns' were")
            }
            else {
                errors.add("Expected '${doc.expectedColumns}' to be primary key, but '$columns' were")
            }
        }
        else {
            verified = true
        }

        return errors
    }
}

class UniqueVerifier(constraintName: FullConstraintName, val columns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    constructor(uniqueConstraint: UniqueConstraint) : this(uniqueConstraint.constraintName, uniqueConstraint.columnNames)

    fun verify(doc: UniqueDoc): List<String> {
        val errors = mutableListOf<String>()
        if(doc.expectedColumns!=columns) {
            if(constraintName!=null) {
                errors.add("Expected '${doc.expectedColumns}' to be unique constraint '$constraintName', but '$columns' are the primary key")
            }
            else {
                errors.add("Expected '${doc.expectedColumns}' to be unique constraint, but '$columns' were expected")
            }
        }
        else {
            verified = true
        }
        return errors
    }
}

class CheckVerifier(constraintName: FullConstraintName, val columns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    constructor(checkConstraint: CheckConstraint) : this(checkConstraint.constraintName, checkConstraint.columnNames)

    fun verify(doc: CheckConstraintDoc): List<String> {
        return if(doc.columns!=columns) {
            listOf("Check constraint '$constraintName' is expected to check '${doc.columns}' but actually is checking '${columns}'")
        }
        else {
            listOf()
        }
    }
}

class ForeignKeyVerifier(constraintName: FullConstraintName, val actualMapping: Map<FullColumnName,FullColumnName>) : ConstraintVerifier(constraintName) {
    constructor(fk: ForeignKeyConstraint) : this(fk.constraintName, fk.mapping)

    fun verify(doc: ForeignKeyDoc): List<String> {
        val errors = mutableListOf<String>()

        if(doc.expectedMapping!=actualMapping) {
            errors.add("Expected '$constraintName' is '${doc.expectedMapping}', but they actual was '$actualMapping'")
        }

        verified = errors.isEmpty()

        return errors
    }
}

class ColumnVerifier(val columnName: FullColumnName, val dataType: String, val isNullable: Boolean) : Verifier() {
    constructor(columnInfo: ColumnDBInfo) : this(columnInfo.name, columnInfo.dataType, columnInfo.isNullable)

    fun verify(doc: ColumnDoc): List<String> {
        fun asString(nullability: Boolean) =  if(nullability) "nullable" else "not nullable"

        val errors = mutableListOf<String>()
        if(isNullable!=doc.expectedNullability) {
            errors.add("Column '$columnName' is expected to be ${asString(doc.expectedNullability)} but is ${asString(isNullable)}")
        }

        if(doc.expectedDataType!=dataType) {
            errors.add("Column '$columnName' is expected to be ${doc.expectedDataType} but is $dataType")
        }

        verified = errors.isEmpty()
        return errors
    }
}

fun <T> List<T>.addTo(other: MutableList<T>) = other.addAll(this)
