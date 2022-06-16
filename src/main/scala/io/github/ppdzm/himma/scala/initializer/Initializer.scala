package io.github.ppdzm.himma.scala.initializer

trait Initializer {
    def initialize(url: String, database: String, table: String, primaryKeys: Array[String], fields: Array[String]): Unit
}
