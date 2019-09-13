package io.github.dibog.jdbcdoc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource


internal fun setupDataSource(): DataSource {
    return HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = "jdbc:hsqldb:mem:unittest"
//                jdbcUrl = "jdbc:hsqldb:file:embedded/hsqldb;shutdown=true"
                username = "root"
                password = "root"
            }
    )
}

internal fun setupIsrDocTestDataSource(): DataSource {
    return HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = "jdbc:postgresql://localhost/isr"
                username = "isr"
                password = "isrdb213"
            }
    )
}
