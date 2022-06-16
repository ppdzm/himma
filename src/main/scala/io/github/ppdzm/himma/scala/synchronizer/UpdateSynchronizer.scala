package io.github.ppdzm.himma.scala.synchronizer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.hadoop.hbase.HBaseEnvironment
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.feature.LoanPattern
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
class UpdateSynchronizer(himma: Himma) extends HBaseEnvironment {

    import himma._
    import himma.himmaConfig._

    private lazy val hBaseContext = new HBaseContext(sparkSession.sparkContext, configuration)
    private lazy val logging = new Logging(getClass)
    override protected val zookeeperQuorum: String = ZOOKEEPER_QUORUM.stringValue()
    override protected val zookeeperPort: Int = ZOOKEEPER_PORT.intValue()

    /**
     * 批量方式同步更新事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeUpdate(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        val entryHavingKey = e.rowDataList
        this.logging.logDebug(entryHavingKey.length + " rows data will be updated")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = entryHavingKey.map(e => {
                val afterColumnsList = e.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                afterColumnsList.filter(_.isNull).filter(_.updated).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), "".getBytes))
                put
            }).asJava
            hBaseTable.put(puts)
        })
    }

    /**
     * 标记数据为更新
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsUpdate(e: HimmaEntry, hTable: String, family: String) = {
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
                afterColumnsList.filter(_.isNull).filter(_.updated).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), "".getBytes))
                val result = hbaseHandler.get(hTable, rowKey)
                if (result.isNull || result.listCells().isNull) {
                    put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, "UPDATED".getBytes)
                }
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

    /**
     * 批量方式同步更新事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeUpdate(e: List[HimmaEntry], hTable: String, family: String) = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList)
        if (entryHavingKey.lengthCompare(1) > 0)
            this.logging.logInfo(entryHavingKey.length + " rows data will be updated")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = entryHavingKey.map(e => {
                val afterColumnsList = e.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                afterColumnsList.filter(_.isNull).filter(_.updated).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), "".getBytes))
                put
            }).asJava
            hBaseTable.put(puts)
        })
    }

    /**
     * 标记数据为更新
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsUpdate(e: List[HimmaEntry], hTable: String, family: String) = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList)
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = new util.ArrayList[Put](entryHavingKey.length)
            entryHavingKey.foreach(e => {
                val afterColumnsList = e.afterColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, afterColumnsList)
                val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                val put = new Put(Bytes.toBytes(rowKey))
                afterColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                afterColumnsList.filter(_.isNull).filter(_.updated).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), "".getBytes))
                val result = hbaseHandler.get(hTable, rowKey)
                if (result.isNull || result.listCells().isNull) {
                    put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, "UPDATED".getBytes)
                }
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

}
