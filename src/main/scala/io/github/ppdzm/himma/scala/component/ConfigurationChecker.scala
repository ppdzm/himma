package io.github.ppdzm.himma.scala.component

import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.database.pool.mysql.MySQLHandlerPool
import io.github.ppdzm.utils.universal.alert.Alerter
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.base.Symbols.lineSeparator
import io.github.ppdzm.utils.universal.feature.{ExceptionGenerator, Pool}

import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by Stuart Alex on 2017/2/9.
 */
case class ConfigurationChecker(himmaConfig: HimmaConfig) {

    import himmaConfig._

    private lazy val logging = new Logging(getClass)
    private lazy val errorList = mutable.ListBuffer[String]()

    /**
     * 检查配置文件、MySQL、Canal日志解析的有效性
     */
    def check(alerter: Alerter): Unit = {
        if (HIMMA_CONFIG_CHECK.booleanValue)
            this.checkConfig()
        if (HIMMA_MYSQL_CONFIG_CHECK.booleanValue)
            this.checkMySQL()
        if (HIMMA_CANAL_CONFIG_CHECK.booleanValue)
            this.checkCanal()
        if (this.errorList.nonEmpty) {
            this.errorList.foreach(e => this.logging.logError(e, null))
            val subject = s"【Himma Application ${HIMMA_INSTANCE.stringValue} 配置检查失败】"
            val content = s"${this.errorList.mkString(lineSeparator)}"
            val exception = ExceptionGenerator.newException("PropertyNotPerfect", s"Configuration check failed")
            alerter.alert(subject, content, exception)
            throw exception
        } else {
            this.logging.logInfo("Configuration check pass")
        }
    }

    private def checkConfig(): Unit = {
        if (HBASE_NAMESPACE.stringValue == "default")
            this.errorList += s"${HBASE_NAMESPACE.getKey} can't be default"
        if (!CANAL_INSTANCE.isDefined || CANAL_INSTANCE.stringValue.isEmpty)
            this.errorList += s"${CANAL_INSTANCE.getKey} is missing"
        if (!CANAL_ADDRESS.isDefined || CANAL_ADDRESS.stringValue.isEmpty)
            this.errorList += s"${CANAL_ADDRESS.getKey} is missing"
        if (!CANAL_PORT.isDefined || CANAL_PORT.stringValue.isEmpty)
            this.errorList += s"${CANAL_PORT.getKey} is missing"
        if (!HIMMA_SYNC_FILTER.isDefined || CANAL_ADDRESS.stringValue.isEmpty)
            this.errorList += s"${CANAL_ADDRESS.getKey} is missing"
    }

    private def checkMySQL(): Unit = {
        if (!MYSQL_HOST.isDefined || MYSQL_HOST.stringValue.isEmpty)
            this.errorList += s"${MYSQL_HOST.getKey} is missing"
        if (!MYSQL_PORT.isDefined || MYSQL_PORT.stringValue.isEmpty)
            this.errorList += s"${MYSQL_PORT.getKey} is missing"
        if (!MYSQL_USERNAME.isDefined || MYSQL_USERNAME.stringValue.isEmpty)
            this.errorList += s"${MYSQL_USERNAME.getKey} is missing"
        if (!MYSQL_PASSWORD.isDefined || MYSQL_PASSWORD.stringValue.isEmpty)
            this.errorList += s"${MYSQL_PASSWORD.getKey} is missing"
        himmaConfig.getDatabases.foreach {
            databaseAlias =>
                val databaseRealName = himmaConfig.getDatabaseName(databaseAlias)
                val url = himmaConfig.getMySQLUrl(databaseAlias)
                val binlogFormat = Pool.borrow(MySQLHandlerPool(url, null))(_.binlogFormat())
                if (binlogFormat.toLowerCase != "row") {
                    this.errorList += s"binlog_format of database $databaseRealName is not set to ROW, this may cause great problem"
                }
                val tableGroupAliasList = himmaConfig.getTableGroupAliasList(databaseAlias)
                tableGroupAliasList.foreach {
                    tableGroupAlias =>
                        himmaConfig.getTables(databaseAlias, tableGroupAlias).foreach {
                            realTable =>
                                val logicalPrimaryKeys = himmaConfig.getLogicalPrimaryKeys(databaseAlias, tableGroupAlias, realTable)
                                if (logicalPrimaryKeys.isEmpty)
                                    this.errorList += s"can not find primary key of table $realTable"
                        }
                }
        }
    }

    private def checkCanal(): Unit = {
        val canalAddress = new InetSocketAddress(CANAL_ADDRESS.stringValue, CANAL_PORT.intValue)
        val connector = CanalConnectors.newSingleConnector(canalAddress, CANAL_INSTANCE.stringValue, "canal", "canal")
        connector.connect()
        connector.subscribe(HIMMA_SYNC_FILTER.stringValue)
        connector.rollback()
        val message = connector.getWithoutAck(HIMMA_SYNC_BATCH_SIZE.intValue)
        val id = message.getId
        val entries = message.getEntries.filter(e => e.getEntryType != EntryType.TRANSACTIONBEGIN && e.getEntryType != EntryType.TRANSACTIONEND)
        val size = entries.length
        if (id == -1 || size == 0) {
            this.logging.logWarning(s"Can't get any message from canal instance ${CANAL_INSTANCE.stringValue}, probably there is something wrong with it")
        } else {
            entries.foreach {
                entry => HimmaEntry.display(entry)
            }
        }
        connector.disconnect()
    }

}
