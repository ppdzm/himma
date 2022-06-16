package io.github.ppdzm.himma.scala.synchronizer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.hadoop.hbase.HBaseEnvironment
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.feature.LoanPattern
import io.github.ppdzm.utils.universal.implicits.BasicConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes

import java.util
import scala.collection.JavaConverters._

/**
 * Created by Stuart Alex on 2017/12/19.
 */
@Deprecated
class DeleteSynchronizer(himma: Himma) extends HBaseEnvironment {

    import himma._
    import himma.himmaConfig._

    private lazy val hBaseContext = new HBaseContext(sparkSession.sparkContext, configuration)
    private lazy val logging = new Logging(getClass)
    override protected val zookeeperQuorum: String = ZOOKEEPER_QUORUM.stringValue()
    override protected val zookeeperPort: Int = ZOOKEEPER_PORT.intValue()

    /**
     * 批量方式同步删除事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeDelete(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        val entryHavingKey = e.rowDataList
        this.logging.logDebug(entryHavingKey.length + " rows data will be deleted")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val deletes = entryHavingKey.map(e => {
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, e.beforeColumns)
                val delete = new Delete(Bytes.toBytes(himma.generateRowKey(database, sourceTable, primaryKeyValues)))
                delete
            }).asJava
            hBaseTable.delete(deletes)
        })
    }

    /**
     * 标记数据被删除
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsDelete(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        val entryHavingKey = e.rowDataList.filter(e => e.beforeColumns.exists(_.isKey))
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = new util.ArrayList[Put](entryHavingKey.size)
            entryHavingKey.foreach(e => {
                val beforeColumnsList = e.beforeColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, beforeColumnsList)
                val put = new Put(Bytes.toBytes(himma.generateRowKey(database, sourceTable, primaryKeyValues)))
                beforeColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, "DELETED".getBytes)
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

    /**
     * 批量方式同步删除事件
     *
     * @param e      解析后的MySQL Canal Entry list
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeDelete(e: List[HimmaEntry], hTable: String, family: String): Unit = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList)
        if (entryHavingKey.lengthCompare(1) > 0)
            this.logging.logInfo(entryHavingKey.length + " rows data will be deleted")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val deletes = entryHavingKey.map(entry => {
                val cpk = himma.getLogicalPrimaryKeyValues(database, sourceTable, entry.beforeColumns)
                val delete = new Delete(Bytes.toBytes(himma.generateRowKey(database, sourceTable, cpk)))
                delete
            }).asJava
            hBaseTable.delete(deletes)
        })
    }

    /**
     * 标记数据被删除
     *
     * @param e      解析后的MySQL Canal Entry list
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def markAsDelete(e: List[HimmaEntry], hTable: String, family: String) = {
        val database = e.head.database
        val sourceTable = e.head.table
        val entryHavingKey = e.flatMap(_.rowDataList.filter(entry => entry.beforeColumns.exists(_.isKey)))
        this.logging.logDebug(entryHavingKey.length + " rows data will be deleted")
        LoanPattern.using(connection.getTable(TableName.valueOf(hTable)))(hBaseTable => {
            val puts = new util.ArrayList[Put](entryHavingKey.size)
            entryHavingKey.foreach(entry => {
                val beforeColumnsList = entry.beforeColumns
                val primaryKeyValues = himma.getLogicalPrimaryKeyValues(database, sourceTable, beforeColumnsList)
                val put = new Put(Bytes.toBytes(himma.generateRowKey(database, sourceTable, primaryKeyValues)))
                beforeColumnsList.filterNot(_.isNull).foreach(c => put.addColumn(Bytes.toBytes(family), Bytes.toBytes(c.columnName.toLowerCase), Bytes.toBytes(c.value.replaceSpecialSymbol)))
                put.addColumn(Bytes.toBytes(family), FINAL_STATUS_COLUMN_NAME.bytesValue, Bytes.toBytes("DELETED"))
                puts.add(put)
            })
            hBaseTable.put(puts)
        })
    }

}
