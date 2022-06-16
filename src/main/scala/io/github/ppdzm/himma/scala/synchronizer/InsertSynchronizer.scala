package io.github.ppdzm.himma.scala.synchronizer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.hadoop.hbase.HBaseEnvironment
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.feature.LoanPattern
import io.github.ppdzm.utils.universal.implicits.ArrayConversions._
import io.github.ppdzm.utils.universal.implicits.BasicConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes

import java.util
import scala.collection.JavaConverters._

/**
 * Created by Stuart Alex on 2017/12/19.
 */
@Deprecated
class InsertSynchronizer(himma: Himma) extends HBaseEnvironment {

    import himma._
    import himma.himmaConfig._

    private lazy val hBaseContext = new HBaseContext(sparkSession.sparkContext, configuration)
    private lazy val logging = new Logging(getClass)
    override protected val zookeeperQuorum: String = ZOOKEEPER_QUORUM.stringValue()
    override protected val zookeeperPort: Int = ZOOKEEPER_PORT.intValue()

    /**
     * 批量方式同步插入事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeInsert(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        val entryHavingKey = e.rowDataList
        this.logging.logDebug(s"${entryHavingKey.length} rows data will be inserted")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = entryHavingKey.map(e => {
                val afterColumnsList = e.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put
            }).asJava
            hBaseTable.put(puts)
        })
    }

    /**
     * 标记新插入数据
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsInsert(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        val entryHavingKey = e.rowDataList
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = new util.ArrayList[Put](entryHavingKey.length)
            entryHavingKey.foreach(e => {
                val afterColumnsList = e.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, "INSERTED".getBytes)
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

    /**
     * 批量方式同步插入事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeInsert(e: List[HimmaEntry], hTable: String, family: String) = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList)
        if (entryHavingKey.lengthCompare(1) > 0)
            this.logging.logInfo(s"${entryHavingKey.length} rows data will be inserted")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = entryHavingKey.map(entry => {
                val afterColumnsList = entry.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put
            }).asJava
            hBaseTable.put(puts)
        })
    }

    /**
     * 标记新插入数据
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsInsert(e: List[HimmaEntry], hTable: String, family: String) = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList)
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = new util.ArrayList[Put](entryHavingKey.size)
            entryHavingKey.foreach(entry => {
                val afterColumnsList = entry.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, "INSERTED".getBytes)
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

    /**
     * 新增数据写入累加表
     *
     * @param e      解析后的MySQL
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    @Deprecated
    def accumulateBeforeSynchronizeInsert(e: HimmaEntry, hTable: String, family: String) = {
        val tableNameWithoutNamespace = hTable.replace(s"${HBASE_NAMESPACE.stringValue}:", "")
        val aggregateColumns = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.aggregate.columns", "").arrayValue(",").map(_.trim).filter(_.notNullAndEmpty).ascending
        if (aggregateColumns.nonEmpty) {
            val database = e.database
            val sourceTable = e.table
            val entryHavingKey = e.rowDataList
            val suffix = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.suffix", "accumulation").stringValue()
            val tableName = TableName.valueOf(hTable + "_" + suffix)
            val accumulationColumns = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.columns").arrayValue(",").map(_.trim).filter(_.notNullAndEmpty).ascending
            LoanPattern.using(connection.getTable(tableName))(hBaseTable => {
                entryHavingKey.foreach(e => {
                    val afterColumnsList = e.afterColumns
                    val rowKey = himma.generateRowKey(database, sourceTable, aggregateColumns.map(c => afterColumnsList.find(_.columnName == c).get.value))
                    val masterRK = himma.generateRowKey(database, sourceTable, himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList))
                    val masterR = hbaseHandler.get(hTable, masterRK)
                    if (masterR.isNull || masterR.listCells().isNull) {
                        accumulationColumns.foreach(column => {
                            hBaseTable.incrementColumnValue(rowKey.getBytes, family.getBytes, (column + "_accumulation").getBytes, afterColumnsList.find(_.columnName == column).get.value.toLong)
                        })
                    }
                })
            })
        }
    }

    /**
     * 新增数据写入计数表
     *
     * @param e      解析后的MySQL
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    @Deprecated
    def countBeforeSynchronizeInsert(e: HimmaEntry, hTable: String, family: String) = {
        val tableNameWithoutNamespace = hTable.replace(s"${HBASE_NAMESPACE.stringValue}:", "")
        val columns = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.count.aggregate.columns").arrayValue(",").map(_.trim).filter(_.notNullAndEmpty).ascending
        if (columns.nonEmpty) {
            val database = e.database
            val sourceTable = e.table
            val entryHavingKey = e.rowDataList
            val suffix = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.count.suffix", "count").stringValue()
            val tableName = TableName.valueOf(hTable + "_" + suffix)
            LoanPattern.using(connection.getTable(tableName))(hBaseTable => {
                entryHavingKey.foreach(e => {
                    val afterColumnsList = e.afterColumns
                    val rowKey = himma.generateRowKey(database, sourceTable, columns.map(c => afterColumnsList.find(_.columnName == c).get.value))
                    val masterRK = himma.generateRowKey(database, sourceTable, himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList))
                    val masterR = hbaseHandler.get(hTable, masterRK)
                    if (masterR.isNull || masterR.listCells().isNull)
                        hBaseTable.incrementColumnValue(rowKey.getBytes, family.getBytes, "count".getBytes, 1)
                })
            })
        }
    }

}
