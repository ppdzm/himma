package io.github.ppdzm.himma.scala.sink

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry

case class MySQLSink(himma: Himma) extends Sink {

    import himma.himmaConfig.MYSQL_SINK_ENABLED

    override def sink(e: HimmaEntry): Unit = {
        if (!MYSQL_SINK_ENABLED.booleanValue)
            return
        //TODO
    }
}
