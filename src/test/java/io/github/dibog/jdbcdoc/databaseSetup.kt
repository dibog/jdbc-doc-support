package io.github.dibog.jdbcdoc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource


internal fun setupDataSource(): DataSource {
    return HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = "jdbc:hsqldb:mem:unittest"
                username = "root"
                password = "root"
            }
    )
}
