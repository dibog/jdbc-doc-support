package io.github.dibog.jdbcdoc.entities

data class CheckConstraintChecker(val fullConstraintName: FullConstraintName, val constraints: Set<TmpCheckConstraint>)
data class PrimaryKeyConstraintChecker(val fullConstraintName: FullConstraintName, val columns: Set<FullColumnName>)
data class UniqueConstraintChecker(val fullConstraintName: FullConstraintName, val columns: Set<FullColumnName>)
data class ForeignKeyConstraintChecker(val fullConstraintName: FullConstraintName, val constraints: Set<TmpForeignKeyConstraint>)
