package io.github.dibog.jdbcdoc

import org.junit.jupiter.api.Test

class TestSqlDialect {
    val ds = setupDataSource()

    @Test
    fun dialect() {
        ds.connection.use { conn ->
            with(conn.metaData) {
                this.getPrimaryKeys("isr","test","foo1").dump()
                this.getExportedKeys("isr", "test", "foo1").dump()
                this.getImportedKeys("isr", "test", "foo1").dump()
                println()

                this.getPrimaryKeys("isr","test","foo2").dump()
                this.getExportedKeys("isr", "test", "foo2").dump()
                this.getImportedKeys("isr", "test", "foo2").dump()
                println()

                this.getPrimaryKeys("isr","test","foo3").dump()
                this.getExportedKeys("isr", "test", "foo3").dump()
                this.getImportedKeys("isr", "test", "foo3").dump()
                println()

                this.getPrimaryKeys("isr","test","foo4").dump()
                this.getExportedKeys("isr", "test", "foo4").dump()
                this.getImportedKeys("isr", "test", "foo4").dump()
                println()
            }
        }
    }
}
