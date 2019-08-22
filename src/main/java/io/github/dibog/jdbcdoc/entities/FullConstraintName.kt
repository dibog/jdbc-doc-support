package io.github.dibog.jdbcdoc.entities

data class FullConstraintName(val catalog: String, val schema: String, val constraint: String) {
    override fun toString() = "$catalog.$schema.$constraint"
}