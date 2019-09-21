package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullTableName

/** Column Info object extracted from the data base. */
class TableDBInfo(
        val tableName: FullTableName,
        val columns: List<ColumnDBInfo>,
        val primaryKey: PrimaryKeyConstraint?,
        val uniques: List<UniqueConstraint>,
        val checks: List<CheckConstraint>,
        val foreignKeys: List<ForeignKeyConstraint>
)


/** Column Info object extracted from the data base. */
data class ColumnDBInfo(val name: FullColumnName, val dataType: String, val isNullable: Boolean, val position: Int)
