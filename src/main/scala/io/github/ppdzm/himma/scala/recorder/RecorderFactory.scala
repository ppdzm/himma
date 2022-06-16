package io.github.ppdzm.himma.scala.recorder

import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.himma.scala.recorder.impl.{HBaseMappingRecorder, HBaseSignalRecorder}

object RecorderFactory {

    def getMappingRecorder(recorderType: String,himmaConfig: HimmaConfig):MappingRecorder = {
        RecorderType(recorderType) match {
            case RecorderType.HBASE => HBaseMappingRecorder(himmaConfig)
            case _ => throw new Exception(s"unsupported mapping recorder type ${recorderType.toString}")
        }
    }

    def getSignalRecorder(recorderType: String,himmaConfig: HimmaConfig):SignalRecorder = {
        RecorderType(recorderType) match {
            case RecorderType.HBASE => HBaseSignalRecorder(himmaConfig)
            case _ => throw new Exception(s"unsupported mapping recorder type ${recorderType.toString}")
        }
    }

}
