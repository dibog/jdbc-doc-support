package io.github.dibog.jdbcdoc

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
                errors.add("Expected column '$name' to be '${userCol.dataType}' but actual was ${dbCol.dataType}")
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

            else -> {
                val userPk = userTable.primarykey!!
                val dbPk = dbTable.primaryKey!!
                if(dbTable.primaryKey!!.constraintName!=userTable.primarykey!!.constraintName) {
                    errors.add("Expected primary key to be named '${userPk.constraintName}, but actually was named '${dbPk.constraintName}")
                }

                if(dbTable.primaryKey!!.columnNames!=userTable.primarykey!!.columnNames) {
                    errors.add("Expected primary key '${userPk.constraintName}' on '${userPk.columnNames.map { it.column }}', but actual was on '${dbPk.columnNames.map { it.column }}'.")
                }
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
            val dbUniqueKey = dbTable.uniques.firstOrNull { it.constraintName==name }
            if(dbUniqueKey!=null) {
                val userUniqueKey = userTable.uniqueKeys[name]!!

                if (dbUniqueKey.columnNames != userUniqueKey.columnNames) {
                    errors.add("Expected unique key '$name' to be on '${userUniqueKey.columnNames.map { it.column }}' but actual was on '${dbUniqueKey.columnNames.map { it.column }}'")
                }
            }
        }
    }

    private fun verifyForeignKey() {
        val dbForeignKeys = dbTable.foreignKeys.map { it.constraintName }
        val userForeignKeys = userTable.foreignKeys.keys
        val existsOnlyInDb = dbForeignKeys - userForeignKeys
        val existsOnlyInUser = userForeignKeys - dbForeignKeys

        existsOnlyInDb.forEach { key ->
            errors.add("Found unexpected foreign key '$key'")
        }

        existsOnlyInUser.forEach { key ->
            errors.add("Expected foreign key '$key' does not exist")
        }

        userForeignKeys.forEach { name ->
            val dbForeignKey = dbTable.foreignKeys.firstOrNull() { it.constraintName==name }
            if(dbForeignKey!=null) {
                val userForeignKey = userTable.foreignKeys[name]!!

                if (dbForeignKey.mapping != userForeignKey.mapping) {
                    errors.add("Expected unique key '$name' to be on '${userForeignKey.mapping.map { it.value.column }}' but actual was on '${dbForeignKey.mapping.map { it.value.column }}'")
                }
            }
        }
    }

    private fun verifyCheckConstraints() {
        val dbCheckConstraints = dbTable.checks.map { it.constraintName }
        val userCheckConstraints = userTable.checkConstraints.keys

        val existsOnlyInDb = (dbCheckConstraints - userCheckConstraints)
        val existsOnlyInUser = userCheckConstraints - dbCheckConstraints

        existsOnlyInDb.forEach { key ->
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
