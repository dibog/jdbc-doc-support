package io.github.dibog.jdbcdoc.entities

data class TmpCheckConstraint(val fullColumnName: FullColumnName, val fullConstraintName: FullConstraintName, val checkClause: String)

data class TmpPrimaryKeyConstraint(val fullColumnName: FullColumnName, val fullConstraintName: FullConstraintName)

data class TmpUniqueConstraint(val fullColumnName: FullColumnName, val fullConstraintName: FullConstraintName)

internal data class TmpPKOrUniqueConstraint(val fullColumnName: FullColumnName, val fullConstraintName: FullConstraintName) {
    fun toPrimaryKeyConstraint() = TmpPrimaryKeyConstraint(fullColumnName, fullConstraintName)
    fun toUniqueConstraint() = TmpUniqueConstraint(fullColumnName, fullConstraintName)
}

data class TmpForeignKeyConstraint(val fullSrcColumnName: FullColumnName, val fullDestColumnName: FullColumnName, val fullConstraintName: FullConstraintName)

data class TmpColumnInfo(val columnName: FullColumnName, val dataType: String, val isNullable: Boolean)
