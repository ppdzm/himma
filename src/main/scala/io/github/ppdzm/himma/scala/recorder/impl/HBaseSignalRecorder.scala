package io.github.ppdzm.himma.scala.recorder.impl

import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.himma.scala.recorder.SignalRecorder
import io.github.ppdzm.utils.hadoop.constants.ZookeeperConfigConstants
import io.github.ppdzm.utils.hadoop.hbase.HBaseHandler
import io.github.ppdzm.utils.universal.config.Config
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
 * 多表合并时记录00:00:00后的ready状态
 * Created by Stuart Alex on 2017/4/25.
 */
private[recorder] case class HBaseSignalRecorder(himmaConfig: HimmaConfig) extends SignalRecorder with ZookeeperConfigConstants {

    import himmaConfig._

    override protected implicit val config: Config = himmaConfig.config

    private lazy val hBaseHandler = new HBaseHandler(ZOOKEEPER_QUORUM.stringValue(), ZOOKEEPER_PORT.intValue())

    /**
     * 初始化
     *
     */
    def initialize(): Unit = hBaseHandler.createTable(SIGNAL_HBASE_STORAGE.stringValue, HBASE_COLUMN_FAMILY.arrayValue(), null, null, 0)

    /**
     * 设置为准备完毕
     *
     * @return
     */
    def set(signal: String): Unit = {
        hBaseHandler.put(SIGNAL_HBASE_STORAGE.stringValue, SIGNAL_IDENTITY.stringValue + signal, HBASE_COLUMN_FAMILY.stringValue, Map(HIMMA_INSTANCE.stringValue -> "OK"))
    }

    /**
     * 重置
     *
     * @return
     */
    def reset(signal: String): Unit = hBaseHandler.delete(SIGNAL_HBASE_STORAGE.stringValue, SIGNAL_IDENTITY.stringValue + signal, null, null, 0)

    /**
     * 检查是否全部准备完毕
     *
     * @return
     */
    def isReady(signal: String): Boolean = {
        val r = hBaseHandler.get(SIGNAL_HBASE_STORAGE.stringValue, SIGNAL_IDENTITY.stringValue + signal)
        SIGNAL_READY_NUMBER.intValue == r.listCells().length
    }

    /**
     * 获取当前实例是否是第一个完成信号设置的实例
     *
     * @param signal 信号值
     * @return
     */
    override def elect(signal: String): Boolean = {
        val firstReadyQualifier = hBaseHandler.get(SIGNAL_HBASE_STORAGE.stringValue, SIGNAL_IDENTITY.stringValue + signal)
          .listCells()
          .map(cell => (CellUtil.cloneQualifier(cell), cell.getTimestamp))
          .minBy(_._2)
          ._1
        Bytes.toString(firstReadyQualifier) == HIMMA_INSTANCE.stringValue
    }

}
