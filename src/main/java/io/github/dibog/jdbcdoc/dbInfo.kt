package io.github.dibog.jdbcdoc

import io.github.dibog.jdbcdoc.entities.FullColumnName
import io.github.dibog.jdbcdoc.entities.FullConstraintName
import io.github.dibog.jdbcdoc.entities.FullTableName

/** Column Info object extracted from the data base. */
class TableDBInfo(
        val tableName: FullTableName,
        val columns: List<ColumnDBInfo>,
        val primaryKey: PrimaryKeyConstraint?,
        val uniques: List<UniqueConstraint>,
        val checks: List<CheckConstraint>,
        val foreignKeys: List<ForeignKeyConstraint>
) {
    private val shortCuts : Map<FullConstraintName,String>

    init {
        val map = mutableMapOf<FullConstraintName,String>()
        primaryKey?.let {
            map[it.constraintName] = "PK"
        }

        uniques.forEachIndexed { index, c ->
            map[c.constraintName] = "UC$index"
        }

        checks.forEachIndexed { index, c ->
            map[c.constraintName] = "CC$index"
        }

        foreignKeys.forEachIndexed { index, c ->
            map[c.constraintName] = "FK$index"
        }
        shortCuts = map
    }

    fun shortCuts(constraint: FullConstraintName): String? {
        return shortCuts[constraint]
    }

    fun getCheckConstraint(constraint: FullConstraintName) = checks.firstOrNull { it.constraintName==constraint  }
    fun getForeignkey(constraint: FullConstraintName) = foreignKeys.firstOrNull { it.constraintName==constraint  }
    fun getUniqueKey(constraint: FullConstraintName) = uniques.firstOrNull { it.constraintName==constraint  }

    fun getNonPKColumns(): List<ColumnDBInfo> {
        val pk = primaryKey?.columnNames ?: listOf()
        return columns.filter { it.name !in pk }
    }

    fun getIndiciesShortcuts(column: FullColumnName): String {
        val indicies = mutableListOf<String>()

        primaryKey?.let {
            if(column in it.columnNames) {
                indicies.add("PK")
            }
        }

        uniques.asSequence()
                .filter { column in it.columnNames }
                .mapNotNull { shortCuts(it.constraintName) }
                .forEach { indicies.add(it) }

        checks.asSequence()
                .filter { column in it.columnNames }
                .mapNotNull { shortCuts(it.constraintName) }
                .forEach { indicies.add(it) }

        foreignKeys.asSequence()
                .filter { column in it.mapping.keys }
                .mapNotNull { shortCuts(it.constraintName) }
                .forEach { indicies.add(it) }

        return indicies.joinToString(",") { it }
    }
}


/** Column Info object extracted from the data base. */
data class ColumnDBInfo(val name: FullColumnName, val dataType: String, val isNullable: Boolean, val position: Int)
