package io.github.dibog.jdbcdoc.entities

data class ColumnInfo(val fullColumnName: FullColumnName, val dataType: String, val isNullable: Boolean)

data class PrimaryKeyConstraint(val fullConstraintName: FullConstraintName, val fullColumnNames: List<FullColumnName> ) {
    constructor(fullConstraintName: FullConstraintName, fullColumnNames: FullColumnName ) :
            this(fullConstraintName, listOf(fullColumnNames))
}

data class UniqueConstraint(val fullConstraintName: FullConstraintName, val fullColumnNames: List<FullColumnName> ) {
    constructor(fullConstraintName: FullConstraintName, fullColumnNames: FullColumnName ) :
            this(fullConstraintName, listOf(fullColumnNames))
}

data class CheckConstraint(val fullConstraintName: FullConstraintName, val fullColumnNames: Set<FullColumnName>, val clause: String)
{
    class Builder(private val constraintName: FullConstraintName, private val clause: String) {
        private val columnNames = mutableSetOf<FullColumnName>()

        fun addColumnName(columnName: FullColumnName): Builder {
            this.columnNames.add(columnName)
            return this
        }

        fun build(): CheckConstraint {
            require(columnNames.isNotEmpty())
            return CheckConstraint(constraintName, columnNames, clause)
        }
    }
}

data class ForeignKeyConstraint(val fullConstraintName: FullConstraintName, val srcColumns: List<FullColumnName>, val destColumns: List<FullColumnName>) {
    constructor(fullConstraintName: FullConstraintName, srcColumn: FullColumnName, destColumn: FullColumnName)
            : this(fullConstraintName, listOf(srcColumn), listOf(destColumn))
}

internal data class PKOrUniqueConstraint(val fullConstraintName: FullConstraintName, val fullColumnNames: List<FullColumnName> ) {
    class Builder(private val constraintName: FullConstraintName) {
        private val columnNames = mutableListOf<FullColumnName>()

        fun addColumnName(columnName: FullColumnName): Builder {
            this.columnNames.add(columnName)
            return this
        }

        fun build(): PKOrUniqueConstraint {
            require(columnNames.isNotEmpty())
            return PKOrUniqueConstraint(constraintName, columnNames)
        }
    }

    fun toPrimaryKeyConstraint() = PrimaryKeyConstraint(fullConstraintName, fullColumnNames)
    fun toUniqueConstraint() = UniqueConstraint(fullConstraintName, fullColumnNames)
}