package io.github.dibog.jdbcdoc.entities

class TableVerifier(
        private val tableName: FullTableName,
        private val pkVerifier : PrimaryKeyVerifier?,
        columnVerifiers : Iterable<ColumnVerifier>,
        uniqueVerifiers : Iterable<UniqueVerifier>,
        checkVerifiers : Iterable<CheckVerifier>,
        foreignKeyVerifiers : Iterable<ForeignKeyVerifier>
) {
    private val columnVerifier = columnVerifiers.associateBy { it.columnName }
    private val uniqueVerifier = uniqueVerifiers.associateBy { it.constraintName }
    private val checkVerifier = checkVerifiers.associateBy { it.constraintName }
    private val foreignKeyVerifier = foreignKeyVerifiers.associateBy { it.constraintName }
    private val errorMessages = mutableListOf<String>()

    class Builder(private val name: FullTableName, private val primaryKey: PrimaryKeyConstraintChecker?) {
        private val pkVerifier = if(primaryKey==null) null else {
            val (name,columns) = primaryKey
            PrimaryKeyVerifier(name, columns)
        }
        private val uniqueVerifiers = mutableListOf<UniqueVerifier>()
        private val checkVerifiers = mutableListOf<CheckVerifier>()
        private val foreignKeyVerifiers = mutableListOf<ForeignKeyVerifier>()
        private val columnVerifiers = mutableListOf<ColumnVerifier>()

        fun build(): TableVerifier {
            return TableVerifier(
                    name,
                    pkVerifier,
                    columnVerifiers,
                    uniqueVerifiers,
                    checkVerifiers,
                    foreignKeyVerifiers
                    )
        }

        fun addUniqueVerifier(name: FullConstraintName, columns: List<FullColumnName>): Builder {
            uniqueVerifiers.add(UniqueVerifier(name, columns))
            return this
        }

        fun addCheckVerifier(name: FullConstraintName, columns: Set<FullColumnName>, clause: String): Builder {
            checkVerifiers.add(CheckVerifier(name, columns))
            return this
        }

        fun addForeignKeyVerifier(name: FullConstraintName, srcColumns: List<FullColumnName>, destColumns: List<FullColumnName>): Builder {
            foreignKeyVerifiers.add(ForeignKeyVerifier(name, srcColumns, destColumns))
            return this
        }

        fun addColumnVerifier(name: FullColumnName, dataType: String, isNullable: Boolean): Builder {
            columnVerifiers.add(ColumnVerifier(name, dataType, isNullable))
            return this
        }
    }

    fun verifyColumn(columnName: String, expectedDataType: String, expectedNullability: Boolean) {
        val columnName = tableName.toFullColumnName(columnName)
        val columnInfo = columnVerifier[columnName]
        if(columnInfo==null) {
            errorMessages.add("Unknown column '$columnName")
        }
        else {
            val errors = columnInfo.verify(expectedDataType, expectedNullability)
            if(errors.isNotEmpty()!=null) {
                errorMessages.addAll(errors)
            }
        }
    }

    fun verifyPrimaryKey(constraintName: String?, expectedColumns: List<String>){
        val constraintName = if(constraintName==null) null else tableName.toFullConstraintName(constraintName)
        val expectedColumns = expectedColumns.map { tableName.toFullColumnName(it) }

        if(pkVerifier==null) {
            if (constraintName == null) {
                errorMessages.add("Table '$tableName' is expected to have a primary key, but actual has none")
            } else if (constraintName != null) {
                errorMessages.add("Table '$tableName' is expected to have a primary key '$constraintName', but actual has none")
            }
        }
        else {
            val errors = pkVerifier.verify(constraintName, expectedColumns)
            if(errors.isNotEmpty()) {
                errorMessages.addAll(errors)
            }
        }
    }

    fun verifyUnique(constraintName: String?, expectedColumns: List<String>) {
        val constraintName = if(constraintName==null) null else tableName.toFullConstraintName(constraintName)
        val expectedColumns = expectedColumns.map { tableName.toFullColumnName(it) }

        val verifier = if(constraintName==null) {
            val candidates = uniqueVerifier.filter { (name, uv) -> uv.columns==expectedColumns }.map { (name, uv) -> uv}
            when {
                candidates.isEmpty() -> {
                    errorMessages.add("Could not find any unique constraints for '$expectedColumns'")
                    null
                }
                candidates.size==1 -> candidates[0]
                else -> {
                    errorMessages.add("Found more then one unique constraints for '$expectedColumns")
                    null
                }
            }
        }
        else {
            val verifier = uniqueVerifier[constraintName]
            if(verifier==null) {
                errorMessages.add("Could not find any unique constraint with name '$constraintName")
                null
            }
            else {
                verifier
            }
        }

        val errors = verifier?.verify(constraintName, expectedColumns) ?: listOf()
        if(errors.isNotEmpty()) {
            errorMessages.addAll(errors)
        }
    }

    fun verifyForeignKey(constraintName: String?=null, srcColumn: String, targetTable: String, targetColumn: String) {
        val expectedSrcColumn = tableName.toFullColumnName(srcColumn)
        val targetTable = FullTableName(tableName.catalog, tableName.schema, targetTable.toUpperCase())
        val expectedDestColumn = targetTable.toFullColumnName(targetColumn)

        verifyForeignKey(constraintName, listOf(expectedSrcColumn to expectedDestColumn))
    }

    fun verifyForeignKey(constraintName: String?, expectedColumns: List<Pair<FullColumnName, FullColumnName>>) {
        val constraintName = if(constraintName==null) null else tableName.toFullConstraintName(constraintName)
        val expectedSrcColumns = expectedColumns.map { it.first }
        val expectedDestColumns = expectedColumns.map { it.second }

        val verifier = if(constraintName==null) {
            val candidates = foreignKeyVerifier.filter { (name,verifier) ->
                verifier.srcColumns==expectedSrcColumns
            }.map { (name, verifier) -> verifier}

            when {
                candidates.isEmpty() -> {
                    errorMessages.add("Could not find any foreign key constraints for '$expectedSrcColumns'")
                    null
                }
                candidates.size==1 -> candidates[0]
                else -> {
                    errorMessages.add("Found more then one foreign key constraints for '$expectedSrcColumns")
                    null
                }
            }
        }
        else {
            val fkVerifier = foreignKeyVerifier[constraintName]
            if(fkVerifier==null) {
                errorMessages.add("Could not find any foreign key constraint '$constraintName'")
                null
            }
            else {
                fkVerifier
            }
        }

        val errors = verifier?.verify(constraintName, expectedSrcColumns, expectedDestColumns) ?: listOf()
        if(errors.isNotEmpty()) {
            errorMessages.addAll(errors)
        }
    }

    fun verifyAll(skipCheckedException: Boolean): String {
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
}

open class Verifier {
    var verified = false
        protected set(value) { field = value }
}

open class ConstraintVerifier(val constraintName: FullConstraintName) : Verifier()

class PrimaryKeyVerifier( constraintName: FullConstraintName, val columns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    fun verify(constraintName: FullConstraintName?, expectedColumns: List<FullColumnName>): List<String> {
        val errors = mutableListOf<String>()

        if(expectedColumns!=columns) {
            if(constraintName!=null) {
                errors.add("Expected '$expectedColumns' to be primary key '$constraintName', but '$columns' were")
            }
            else {
                errors.add("Expected '$expectedColumns' to be primary key, but '$columns' were")
            }
        }
        else {
            verified = true
        }

        return errors
    }
}

class UniqueVerifier(constraintName: FullConstraintName, val columns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    fun verify(constraintName: FullConstraintName?, expectedColumns: List<FullColumnName>): List<String> {
        val errors = mutableListOf<String>()
        if(expectedColumns!=columns) {
            if(constraintName!=null) {
                errors.add("Expected '$expectedColumns' to be unique constraint '$constraintName', but '$columns' are the primary key")
            }
            else {
                errors.add("Expected '$expectedColumns' to be unique constraint, but '$columns' were expected")
            }
        }
        else {
            verified = true
        }
        return errors
    }
}

class CheckVerifier(constraintName: FullConstraintName, val columns: Set<FullColumnName>) : ConstraintVerifier(constraintName)
class ForeignKeyVerifier(constraintName: FullConstraintName, val srcColumns: List<FullColumnName>, val destColumns: List<FullColumnName>) : ConstraintVerifier(constraintName) {
    fun verify(
            constraintName: FullConstraintName?,
            expectedSrcColumns: List<FullColumnName>,
            expectedDestColumns: List<FullColumnName>
    ): List<String> {
        val errors = mutableListOf<String>()

        if(expectedSrcColumns!=srcColumns) {
            errors.add("Expected '$constraintName' to have source columns '$expectedSrcColumns', but they were '$srcColumns'")
        }
        if(expectedDestColumns!=destColumns) {
            errors.add("Expected '$constraintName' to have targeted columns '$expectedDestColumns', but they were '$destColumns'")
        }

        verified = errors.isEmpty()

        return errors
    }
}

class ColumnVerifier(val columnName: FullColumnName, val dataType: String, val isNullable: Boolean) : Verifier() {
    fun verify(expectedDataType: String, expectedNullability: Boolean): List<String> {
        fun asString(nullability: Boolean) =  if(nullability) "nullable" else "not nullable"

        val errors = mutableListOf<String>()
        if(isNullable!=expectedNullability) {
            errors.add("Column '$columnName' is expected to be ${asString(expectedNullability)} but is ${asString(isNullable)}")
        }

        if(expectedDataType!=dataType) {
            errors.add("Column '$columnName' is expected to be $expectedDataType but is $dataType")
        }

        verified = errors.isEmpty()
        return errors
    }
}
