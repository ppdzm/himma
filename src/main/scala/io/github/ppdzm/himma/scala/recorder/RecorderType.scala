package io.github.ppdzm.himma.scala.recorder

import io.github.ppdzm.utils.universal.base.Enum

object RecorderType extends Enum {
    val HBASE: Value = Value("hbase")
    val MYSQL: Value = Value("mysql")
}
