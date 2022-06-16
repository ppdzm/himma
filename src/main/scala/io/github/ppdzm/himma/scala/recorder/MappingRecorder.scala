package io.github.ppdzm.himma.scala.recorder

import scala.collection.mutable

/**
 * 记录源表——目标表映射关系
 */
trait MappingRecorder {

    lazy val primaryKeysMapping: mutable.Map[String, Array[String]] = mutable.Map[String, Array[String]]()
    lazy val sourceDestinationMapping: mutable.Map[String, (String, Long)] = mutable.Map[String, (String, Long)]()

    def open()

    def add(source: String, destination: String)

    def exists(source: String): Boolean

    def get(source: String): (String, Long)

    def remove(source: String)

    def rename(before: String, after: String)

    def refresh()

}
