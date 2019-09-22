package io.github.dibog.jdbcdoc.entities

import io.github.dibog.jdbcdoc.Context
import io.github.dibog.jdbcdoc.TableDBInfo
import io.github.dibog.jdbcdoc.TableUserInfo

class TableVerifier(
        private val dbTable: TableDBInfo,
        private val userTable: TableUserInfo,
        private val context: Context
) {
    private val errors = mutableListOf<String>()

    private fun verifyColumns() {
        val dbColumns = dbTable.columns.map { it.name }
        val userColumns = userTable.columnInfos.keys
        val existsOnlyInDb = (dbColumns - userColumns)
        val existsOnlyInUser = userColumns - dbColumns

        existsOnlyInDb.forEach { column ->
            errors.add("Found unexpected column '$column'")
        }

        existsOnlyInUser.forEach { column ->
            errors.add("Expected column '$column' does not exist")
        }

        userColumns.forEach { name ->
            val dbCol = dbTable.columns.first { it.name==name }!!
            val userCol = userTable.columnInfos[name]!!

            if(dbCol.dataType!=userCol.dataType) {
                errors.add("Expected column '$name' to be '${userCol.dataType} but actual was ${dbCol.dataType}")
            }

            if(dbCol.isNullable!=userCol.nullability) {
                val dbNull = if(dbCol.isNullable) "NULLABLE" else "NOT_NULLABLE"
                val userNull = if(userCol.nullability) "NULLABLE" else "NOT_NULLABLE"
                errors.add("Expected column '$name' to be '$userNull' but actual was '$dbNull'")
            }
        }
    }

    private fun verifyPK() {
        when {
            dbTable.primaryKey==null && userTable.primarykey!=null ->
                errors.add("Expected primary key '${userTable.primarykey.constraintName}', but actual there is no primary key")

            dbTable.primaryKey!=null && userTable.primarykey==null ->
                errors.add("Expected there is no primary key, but actual found '${dbTable.primaryKey.constraintName}' ('${dbTable.primaryKey.columnNames.map { it.column }}') ")

            dbTable.primaryKey!!.columnNames!=userTable.primarykey!!.columnNames -> {
                val userPk = userTable.primarykey!!
                val dbPk = dbTable.primaryKey!!
                errors.add("Expected primary key '${userPk.constraintName}' on '${userPk.columnNames.map { it.column }}', but actual was on '${dbPk.columnNames.map { it.column }}'.")
            }

            else -> {
                // all is okay and fine
            }
        }
    }

    private fun verifyUniqueKey() {
        val dbUniqueKeys = dbTable.uniques.map { it.constraintName }
        val userUniqueKeys = userTable.uniqueKeys.keys
        val existsOnlyInDb = dbUniqueKeys - userUniqueKeys
        val existsOnlyInUser = userUniqueKeys - dbUniqueKeys

        existsOnlyInDb.forEach { key ->
            errors.add("Found unexpected unique key '$key'")
        }

        existsOnlyInUser.forEach { key ->
            errors.add("Expected unique key '$key' does not exist")
        }

        userUniqueKeys.forEach { name ->
            val dbUniqueKey = dbTable.uniques.first { it.constraintName==name }!!
            val userUniqueKey = userTable.uniqueKeys[name]!!

            if(dbUniqueKey.columnNames!=userUniqueKey.columnNames) {
                errors.add("Expected unique key '$name' to be on '${userUniqueKey.columnNames.map { it.column }}' but actual was on '${dbUniqueKey.columnNames.map { it.column }}'")
            }
        }
    }

    private fun verifyForeignKey() {
        val dbUniqueKeys = dbTable.foreignKeys.map { it.constraintName }
        val userUniqueKeys = userTable.foreignKeys.keys
        val existsOnlyInDb = dbUniqueKeys - userUniqueKeys
        val existsOnlyInUser = userUniqueKeys - dbUniqueKeys

        existsOnlyInDb.forEach { key ->
            errors.add("Found unexpected foreign key '$key'")
        }

        existsOnlyInUser.forEach { key ->
            errors.add("Expected foreign key '$key' does not exist")
        }

        userUniqueKeys.forEach { name ->
            val dbForeignKey = dbTable.foreignKeys.first { it.constraintName==name }!!
            val userForeignKey = userTable.foreignKeys[name]!!

            if(dbForeignKey.mapping!=userForeignKey.mapping) {
                errors.add("Expected unique key '$name' to be on '${userForeignKey.mapping.map { it.value.column }}' but actual was on '${dbForeignKey.mapping.map { it.value.column }}'")
            }
        }
    }

    private fun verifyCheckConstraints() {
        val dbCheckConstraints = dbTable.checks.map { it.constraintName }
        val userCheckConstraints = userTable.checkConstraints.keys

        val existsOnlyInDb = (dbCheckConstraints - userCheckConstraints)
        val constraintsRegExp = context.suppressCheckConstraints
        val existsOnlyInDbFiltered = if (constraintsRegExp == null) {
            existsOnlyInDb
        } else {
            existsOnlyInDb.filter { !constraintsRegExp.containsMatchIn(it.constraint) }
        }

        val existsOnlyInUser = userCheckConstraints - dbCheckConstraints

        existsOnlyInDbFiltered.forEach { key ->
            errors.add("Found unexpected check constraint '$key'")
        }

        existsOnlyInUser.forEach { key ->
            errors.add("Expected check constraint '$key' does not exist")
        }
    }

    fun verifyAll(): String {
        verifyColumns()
        verifyPK()
        verifyUniqueKey()
        verifyForeignKey()
        verifyCheckConstraints()

        return errors.joinToString("\n") { it }
    }
}
