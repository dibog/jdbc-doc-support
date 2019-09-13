package io.github.dibog.jdbcdoc.entities

import io.github.dibog.jdbcdoc.CheckConstraint
import io.github.dibog.jdbcdoc.ForeignKeyConstraint

data class CheckConstraintChecker(val fullConstraintName: FullConstraintName, val constraints: Set<CheckConstraint>)
data class PrimaryKeyConstraintChecker(val fullConstraintName: FullConstraintName, val columns: List<FullColumnName>)
data class UniqueConstraintChecker(val fullConstraintName: FullConstraintName, val columns: List<FullColumnName>)
data class ForeignKeyConstraintChecker(val fullConstraintName: FullConstraintName, val constraint: ForeignKeyConstraint)
