package io.github.ppdzm.himma.scala.component

import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.himma.scala.entity.HimmaColumn
import io.github.ppdzm.himma.scala.initializer.StockDataInitializer
import io.github.ppdzm.himma.scala.recorder.{MappingRecorder, RecorderFactory, SignalRecorder}
import io.github.ppdzm.himma.scala.sink.{HBaseSink, KafkaSink, MySQLSink}
import io.github.ppdzm.himma.scala.synchronizer.{DDLSynchronizer, DeleteSynchronizer, InsertSynchronizer, UpdateSynchronizer}
import io.github.ppdzm.himma.scala.{initializer, sink}
import io.github.ppdzm.utils.hadoop.hbase.HBaseHandler
import io.github.ppdzm.utils.spark.SparkUtils
import io.github.ppdzm.utils.universal.alert.{Alerter, AlerterFactory}
import io.github.ppdzm.utils.universal.base.Logging
import org.apache.spark.sql.SparkSession

class Himma(implicit val himmaConfig: HimmaConfig) {

    import himmaConfig._

    lazy val sparkSession: SparkSession = SparkUtils.getSparkSession()
    lazy val mappingRecorder: MappingRecorder = RecorderFactory.getMappingRecorder(himmaConfig.HIMMA_MAPPING_RECORDER.stringValue(), himmaConfig)
    lazy val hbaseSink: HBaseSink = HBaseSink(this)
    lazy val signalRecorder: SignalRecorder = RecorderFactory.getSignalRecorder(himmaConfig.HIMMA_SIGNAL_RECORDER.stringValue(), himmaConfig)
    lazy val hbaseHandler: HBaseHandler = HBaseHandler(ZOOKEEPER_QUORUM.stringValue, ZOOKEEPER_PORT.intValue)
    lazy val alerter: Alerter = AlerterFactory.getAlerter(himmaConfig.config)
    lazy val ddlSynchronizer = new DDLSynchronizer(this)
    lazy val deleteSynchronizer = new DeleteSynchronizer(this)
    lazy val insertSynchronizer = new InsertSynchronizer(this)
    lazy val updateSynchronizer = new UpdateSynchronizer(this)
    private lazy val logging = new Logging(getClass)
    private lazy val binlogDataHandler: BinlogDataHandler = BinlogDataHandler(this)
    private lazy val configurationChecker: ConfigurationChecker = ConfigurationChecker(himmaConfig)
    private lazy val stockDataInitializer: StockDataInitializer = initializer.StockDataInitializer(this)
    private lazy val kafkaSink: KafkaSink = sink.KafkaSink(this)
    private lazy val mysqlSink: MySQLSink = MySQLSink(this)

    def start(): Unit = {
        // 检查配置是否完善
        configurationChecker.check(alerter)
        try {
            if (HIMMA_INIT_ENABLED.booleanValue) {
                // 初始化存量数据
                stockDataInitializer.initialize()
            }
            this.logging.logInfo(s"Himma instance ${HIMMA_INSTANCE.stringValue} started")
            if (HIMMA_SYNC_ENABLED.booleanValue) {
                // 同步增量数据
                binlogDataHandler.start()
            }
        } catch {
            case e: Exception =>
                val subject = s"【${HIMMA_INSTANCE.stringValue} 异常中断】"
                val content = e.getMessage
                alerter.alert(subject, content, e)
        }
    }

    /**
     * 生成RowKey
     *
     * @param database         数据库名称
     * @param table            表名称
     * @param primaryKeyValues 物理或逻辑主键
     * @return
     */
    def generateRowKey(database: String, table: String, primaryKeyValues: Array[String]): String = {
        if (HBASE_ROW_KEY_REVERSE_PRIMARY_KEYS.booleanValue)
            s"${primaryKeyValues.mkString(HIMMA_KEY_CONNECTOR.stringValue).reverse}${HIMMA_KEY_CONNECTOR.stringValue}${generateMappingKey(database, table)}"
        else
            s"${primaryKeyValues.mkString(HIMMA_KEY_CONNECTOR.stringValue)}${HIMMA_KEY_CONNECTOR.stringValue}${generateMappingKey(database, table)}"
    }

    /**
     * 源表——目标表映射关系的key
     *
     * @param database 数据库名称
     * @param table    表名称
     * @return
     */
    def generateMappingKey(database: String, table: String): String = {
        s"$MYSQL_HOST:$MYSQL_PORT/$database.$table"
    }

    /**
     * 用来模糊匹配的正则表达式
     *
     * @param database 数据库名称
     * @param table    表名称
     * @return
     */
    def generateRowKeyRegexWithoutPrimaryKeys(database: String, table: String): String = {
        s".+${HIMMA_KEY_CONNECTOR.stringValue}${generateMappingKey(database, table)}"
    }

    /**
     * 找出主键字段值，并连接在一起
     *
     * @param columns List[HimmaColumn]
     * @return
     */
    def getLogicalPrimaryKeyValues(database: String, table: String, columns: List[HimmaColumn]): Array[String] = {
        val logicalPrimaryKeys = mappingRecorder.primaryKeysMapping(generateMappingKey(database, table))
        columns
          // 筛选主键字段
          .filter(c => logicalPrimaryKeys.contains(c.columnName))
          // 获取主键字段名称
          .map(c => (c.columnName, c.value))
          // 排序
          .sortBy(_._1)
          // 获取主键字段值
          .map(_._2)
          .toArray
    }

}
