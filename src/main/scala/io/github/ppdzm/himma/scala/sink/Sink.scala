package io.github.ppdzm.himma.scala.sink

import io.github.ppdzm.himma.scala.entity.HimmaEntry

trait Sink {
    def sink(e: HimmaEntry)
}
