package io.github.ppdzm.himma.scala.sink

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.hadoop.kafka.producer.SimpleKafkaProducer

case class KafkaSink(himma: Himma) extends Sink {

    import himma.himmaConfig._

    override def sink(e: HimmaEntry): Unit = {
        if (!KAFKA_SINK_ENABLED.booleanValue)
            return
        val mappingKey = himma.generateMappingKey(e.database, e.table)
        val synchronizationFinishTime = himma.mappingRecorder.get(mappingKey)._2
        if (e.executeTime > synchronizationFinishTime)
            SimpleKafkaProducer.send(KAFKA_BROKERS.stringValue, HIMMA_INSTANCE.stringValue, e.toString)
    }
}
