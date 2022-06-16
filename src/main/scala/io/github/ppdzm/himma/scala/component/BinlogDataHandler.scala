package io.github.ppdzm.himma.scala.component

import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import io.github.ppdzm.himma.scala.entity.HimmaEntry

import java.net.InetSocketAddress
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Created by Stuart Alex on 2016/12/5.
 * MySQL binlog日志解析消费端
 *
 */
case class BinlogDataHandler(himma: Himma) extends DelayChecker {

    import himma._
    import himma.himmaConfig._

    private lazy val simpleDateFormat =
        if (HBASE_SWITCH_FREQUENCY.stringValue == "daily")
            new SimpleDateFormat("yyyyMMdd")
        else
            new SimpleDateFormat("yyyyMMddHH")
    private lazy val lesDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    /**
     * 记录上一条日志执行的时间
     */
    private val lastExecuteTimeCalendar = Calendar.getInstance()
    //  private val initialTime = 1483200000
    /**
     * 记录当前正在消费的日志执行的时间
     */
    private val currentExecuteTimeCalendar = Calendar.getInstance()
    //  this.lastExecuteTimeCalendar.setTimeInMillis(initialTime)
    private val hbaseNamespace = HBASE_NAMESPACE.stringValue
    /**
     * 未能获取到日志的批次数
     */
    private var sleepingBatches = 0
    private var currentExecuteTime = this.currentExecuteTimeCalendar.getTimeInMillis

    /**
     * 同步
     */
    def start(): Unit = {
        val canalInstance = CANAL_INSTANCE.stringValue
        val connector = if (CANAL_CLUSTER_ENABLED.booleanValue) {
            val clusterAddress = CANAL_ADDRESS.arrayValue(CONFIG_SEPARATOR.stringValue).map(new InetSocketAddress(_, CANAL_PORT.intValue)).toList
            CanalConnectors.newClusterConnector(clusterAddress.asJava, canalInstance, "", "")
        }
        else {
            val canalAddress = new InetSocketAddress(CANAL_ADDRESS.stringValue, CANAL_PORT.intValue)
            CanalConnectors.newSingleConnector(canalAddress, canalInstance, "", "")
        }
        connector.connect()
        val filter = HIMMA_SYNC_FILTER.stringValue
        if (filter.nonEmpty)
            connector.subscribe(filter)
        connector.rollback()
        val result = Try {
            // 初始化信号记录器
            signalRecorder.initialize()
            // 启动延迟检查器
            this.startDelayChecker(this.currentExecuteTime, HIMMA_DELAY_CHECK_INTERVAL.intValue, HIMMA_DELAY_THRESHOLD.intValue, this.delayHandler, this.heartbeat)
            this.logging.logInfo(s"Succeeded on connect Canal Server with instance $canalInstance and with subscribe regulation $filter")
            while (true) {
                val message = connector.getWithoutAck(HIMMA_SYNC_BATCH_SIZE.intValue, HIMMA_SYNC_INTERVAL.longValue, TimeUnit.SECONDS)
                val id = message.getId
                val entries = message.getEntries.filter(e => e.getEntryType != EntryType.TRANSACTIONBEGIN && e.getEntryType != EntryType.TRANSACTIONEND)
                if (id == -1 || entries.isEmpty) {
                    this.sleepingAlert()
                } else {
                    entries.foreach(e => {
                        val executeTime = e.getHeader.getExecuteTime
                        this.sleepingBatches = 0
                        this.lastExecuteTimeCalendar.setTimeInMillis(this.currentExecuteTimeCalendar.getTimeInMillis)
                        this.currentExecuteTimeCalendar.setTimeInMillis(executeTime)
                        this.currentExecuteTime = executeTime
                        this.setProgress(executeTime)
                        // 判断当前日志是否跨时段，分两种情况：
                        // 1. 当前日志的执行时间≤当前时间，且跨时段，这种情况要避免
                        // 2. 当前日志的执行时间>于当前时间，且与上一条日志的执行时间相比较跨时段，则需进行跨时段的处理
                        if (executeTime > this.lastExecuteTimeCalendar.getTimeInMillis &&
                          simpleDateFormat.format(currentExecuteTimeCalendar.getTime) != simpleDateFormat.format(lastExecuteTimeCalendar.getTime)) {
                            this.handleTimeSegmentChange(executeTime)
                        }
                        val entry = HimmaEntry(e)
                        hbaseSink.sink(entry)
                    })
                }
                connector.ack(id)
            }
        }
        if (result.isFailure)
            throw result.failed.get
        connector.disconnect()
    }

    /**
     * 出现跨天/时，在日志消费前进行的处理
     *
     * @param time 时间戳
     */
    private def handleTimeSegmentChange(time: Long): Unit = {
        this.lastExecuteTimeCalendar.setTimeInMillis(time)
        //日志跨天/时，创建增量表和快照
        val signal = simpleDateFormat.format(new Date(time))
        signalRecorder.set(signal)
        while (!signalRecorder.isReady(signal)) {
            // 等待分库分表合并的其他himma进程全部处于ready状态
            this.logging.logInfo("Waiting for other brother himma get ready")
            Thread.sleep(5000)
        }
        this.logging.logInfo("All brother himma is ready to proceed, start make snapshot")
        if (signalRecorder.elect(signal)) {
            //创建增量表
            HBASE_INCREMENT_TABLES.arrayValue(CONFIG_SEPARATOR.stringValue)
              .foreach(destination => {
                  val table = s"$hbaseNamespace:$destination"
                  val incrementTable = s"$hbaseNamespace:${destination}_$signal"
                  this.logging.logInfo(s"Create incremental table “$incrementTable” of HBase Table “$table”")
                  hbaseHandler.createTable(incrementTable, Array(HBASE_COLUMN_FAMILY.stringValue), HBASE_START_KEY.stringValue, HBASE_END_KEY.stringValue, HBASE_REGION_NUMBER.intValue)
                  //创建外部表
                  //this.logging.logInfo(s"Create external HiveOverHBaseTable “${this.mainHiveDatabase}.${destination + "-" + shortIntDate}” of HBase Table “$it”")
                  //HiveUtils.createExternalHiveOverHBaseTableFromTableSchema(this.mainHiveDatabase, destination, this.incrementalHiveDatabase, destination + "_" + shortIntDate, it, HBASE_COLUMN_FAMILY -> this.finalStatusColumnName)
              })
            //创建快照
            HBASE_SNAPSHOT_TABLES.arrayValue(CONFIG_SEPARATOR.stringValue)
              .foreach(destination => {
                  val table = s"$hbaseNamespace:$destination"
                  val snapshot = s"$hbaseNamespace-$destination-$signal"
                  this.logging.logInfo(s"Create Snapshot “$snapshot” of HBase Table “$table”")
                  hbaseHandler.snapshot(table, snapshot)
              })
        }
    }

    /**
     * 无日志告警
     */
    private def sleepingAlert(): Unit = {
        this.sleepingBatches += 1
        if (HIMMA_SLEEPING_ALERT_ENABLED.booleanValue && this.sleepingBatches > HIMMA_SLEEPING_BATCH_THRESHOLD.intValue) {
            val subject = s"【Canal实例${CANAL_INSTANCE.stringValue}日志获取告警】"
            val content = s"已连续${HIMMA_SLEEPING_BATCH_THRESHOLD.intValue}批次未能获取到日志，请检查名为${CANAL_INSTANCE.stringValue}的Canal实例及相关的MySQL"
            alerter.alert(subject, content, null)
            this.sleepingBatches = 0
        }
    }

    /**
     * 检查到延迟时的处理方法
     *
     * @param delay 延迟时长，毫秒
     * @param hour  检测到延迟的小时，24小时制
     */
    private def delayHandler(delay: Long, hour: Int): Unit = {
        if (this.sleepingBatches == 0) {
            this.logging.logWarning(s"Progress of synchronization delayed over ${delay / 60000} minutes")
            if (!HIMMA_DELAY_IGNORED_HOURS.arrayValue(CONFIG_SEPARATOR.stringValue).contains(hour.toString)) {
                val subject = s"【Canal实例${CANAL_INSTANCE.stringValue}同步延迟警告】"
                val content = s"同步进度延迟${delay / 60000}分钟"
                alerter.alert(subject, content, null)
            }
        }
    }

    /**
     * 心跳方法
     */
    private def heartbeat(): Unit = {
        this.logging.logInfo(s"Himma $HIMMA_INSTANCE is running, last event executed at ${this.lesDateFormatter.format(new Date(currentExecuteTime))}, sleeping batches is $sleepingBatches")
    }

}
