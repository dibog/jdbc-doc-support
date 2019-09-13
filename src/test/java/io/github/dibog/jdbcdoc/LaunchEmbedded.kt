package io.github.dibog.jdbcdoc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.hsqldb.lib.FileUtil
import org.hsqldb.lib.FileUtil.getDatabaseFileList
import org.springframework.jdbc.core.JdbcTemplate

private fun initializeDatabase(jdbc: JdbcTemplate) {

    jdbc.execute("""
    create table test.foo1 (
        id int not null,
        name varchar(20) not null,
        CONSTRAINT PK_FOO1 PRIMARY KEY (id),
        CONSTRAINT UC_FOO1_NAME UNIQUE (name)
    );
    """.trimIndent())

    jdbc.execute("""
    create table test.foo2 (
        id int not null,
        foo_id int not null,
        my_check varchar(20),
        CONSTRAINT FK_FOO2_ID FOREIGN KEY (id) REFERENCES test.foo1(id),
        CONSTRAINT PK_FOO2 PRIMARY KEY (id),
        CONSTRAINT CH_CHECK CHECK (my_check!='foo' AND id<20)
    );
    """.trimIndent())

    jdbc.execute("""
    create table test.foo3 (
        id1 int not null,
        id2 int not null,
        CONSTRAINT PK_FOO3 PRIMARY KEY (id1, id2),
        CONSTRAINT FK_FOO3_ID1 FOREIGN KEY (id1) REFERENCES test.foo1(id),
        CONSTRAINT FK_FOO3_ID2 FOREIGN KEY (id2) REFERENCES test.foo2(id),
    );
    """.trimIndent())

    jdbc.execute("""
    create table test.foo4 (
        id1 int not null,
        id2 int not null,
        id3 int not null,
        id4 int not null,
        CONSTRAINT PK_FOO4 PRIMARY KEY (id1, id2),
        CONSTRAINT UQ_FOO4 UNIQUE (id3, id4),
        CONSTRAINT FK_FOO4_ID1 FOREIGN KEY (id1,id2) REFERENCES test.foo3(id1,id2),
        CONSTRAINT FK_FOO4_ID2 FOREIGN KEY (id3,id4) REFERENCES test.foo3(id2,id1)
    );
    """.trimIndent())

}

fun main() {
    val db = "hsqldb/embedded"
//    val databaseFileList = getDatabaseFileList(db)
//    val deleteOrRenameDatabaseFiles = FileUtil.deleteOrRenameDatabaseFiles(db)

    val ds = HikariDataSource(  HikariConfig().apply {
        jdbcUrl = "jdbc:hsqldb:file:$db;shutdown=true"
        username = "root"
        password = "root"
    })

    val jdbc = JdbcTemplate(ds)
    createDatabase(jdbc)
    initializeDatabase(jdbc)
}
