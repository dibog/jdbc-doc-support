package io.github.dibog.jdbcdoc

import java.nio.file.Path
import java.nio.file.Paths

data class Context(
        val suppressCheckConstraints: Regex? = null,
        val suppressTables: Regex? = null,
        val docDir : Path = Paths.get("target/snippets-jdbcdoc")

)
