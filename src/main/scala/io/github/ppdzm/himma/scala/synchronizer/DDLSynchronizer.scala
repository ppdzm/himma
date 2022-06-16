package io.github.ppdzm.himma.scala.synchronizer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry
import io.github.ppdzm.utils.hadoop.hbase.HBaseEnvironment
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.base.Symbols._
import io.github.ppdzm.utils.universal.implicits.ArrayConversions._
import org.apache.hadoop.hbase.client.{Delete, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, FilterList, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Created by Stuart Alex on 2017/12/19.
 */
@Deprecated
class DDLSynchronizer(himma: Himma) extends HBaseEnvironment {

    import himma._
    import himma.himmaConfig._

    private lazy val hBaseContext = new HBaseContext(sparkSession.sparkContext, configuration)
    private lazy val logging = new Logging(getClass)
    override protected val zookeeperQuorum: String = ZOOKEEPER_QUORUM.stringValue()
    override protected val zookeeperPort: Int = ZOOKEEPER_PORT.intValue()

    /**
     * 同步Alter Table事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     */
    def synchronizeAlter(e: HimmaEntry, hTable: String) = {
        val database = e.database
        val sourceTable = e.table
        val ddl = e.ddl.replaceAll("comment.*?'[^']*?'", "")
        val renameTableRegex1 = "alter table (?<before>.+?) rename to (?<after>.+)".r
        val renameTableRegex2 = "alter table (?<before>.+?) rename (?<after>.+)".r
        val renameTableRegex3 = "rename table (details)".r
        val alterIncrementRegex = "alter table (?<before>.+?) auto_increment=(?<after>.+)".r
        val alterColumnRegex = "alter table (?<table>.+?) (?<change>.+)".r
        val result = Try {
            ddl match {
                case renameTableRegex1(before, after) => this.synchronizeRenameTable(hTable, database, sourceTable, before, after)
                case renameTableRegex2(before, after) => this.synchronizeRenameTable(hTable, database, sourceTable, before, after)
                case renameTableRegex3(d) =>
                    val details = d.split(",").map(_.trim).filter(_.nonEmpty).map(_.split("to").map(_.trim))
                    details.foreach(ba => {
                        val before = ba(0)
                        val after = ba(1)
                        this.synchronizeRenameTable(hTable, database, sourceTable, before, after)
                    })
                case alterIncrementRegex(before, after) =>
                case alterColumnRegex(mysqlTable, c) =>
                    val addPKRegex = "(?<irrelevant>.+?)primary key \\((?<key>.+?)\\)(?<other>.*?)".r
                    var change = c
                    val changes = {
                        val precisionRegex = "\\(\\d+,\\d+\\)".r
                        val ms = precisionRegex.findAllMatchIn(change)
                        ms.foreach(m => {
                            val find = m.group(0)
                            change = change.replace(find, find.replace(",", "COMMA_BETWEEN_PRECISION"))
                        })
                    } match {
                        case addPKRegex(irrelevant, key, other) =>
                            change.replace(key, "PRIMARY_KEYS").split(",").map(_.replace("PRIMARY_KEYS", key)).map(_.trim)
                        case _ => change.split(",").map(_.trim).map(_.replace("COMMA_BETWEEN_PRECISION", ","))
                    }
                    val addPartitionRegex = "(?<ignore1>.*?)partition(?<ignore2>.+)".r
                    val addColumnRegex1 = "add column (?<column>.+?) (?<description>.+)".r
                    val addColumnRegex2 = "add (?<column>.+?) (?<description>.+)".r
                    val addPrimaryKeyRegex = "add(?<constraint>.+?)primary key \\((?<key>.+?)\\)".r
                    val changeColumnRegex1 = "change column (?<before>.+?) (?<after>.+?) (?<description>.+)".r
                    val changeColumnRegex2 = "change (?<before>.+?) (?<after>.+?) (?<description>.+)".r
                    val dropIndexRegex = "drop index (?<index>.+?)".r
                    val addIndexRegex = "add index (?<index>.+?)".r
                    val dropColumnRegex1 = "drop column (?<column>.+?)".r
                    val dropColumnRegex2 = "drop (?<column>.+?)".r
                    val dropPrimaryKeyRegex = "drop primary key".r
                    val modifyColumnRegex1 = "modify column (?<column>.+?) (?<description>.+)".r
                    val modifyColumnRegex2 = "modify (?<column>.+?) (?<description>.+)".r
                    val rowKeyWithEmptyPrimaryKeys = himma.generateRowKey(database, sourceTable, Array())
                    changes.foreach {
                        case addIndexRegex(index) =>
                        case dropIndexRegex(index) =>
                        case addPartitionRegex(ignore1, ignore2) =>
                        case addPrimaryKeyRegex(constraint, key) => //添加主键的事件单独出现则忽略 ， 与drop一起出现则已经在drop子DDL同步中处理
                        case dropPrimaryKeyRegex() =>
                            //一般来说，正常的修改主键是先drop和add先后出现的，因此合并对这2个子DDL进行同步，如果只drop但没有add，视为放弃对这张表的同步
                            if (changes.exists {
                                case addPrimaryKeyRegex(constraint, key) => true
                                case _ => false
                            }) {
                                changes.foreach {
                                    case addPrimaryKeyRegex(constraint, key) =>
                                        val keys = key.replace(backQuote, "").split(comma).map(_.trim).ascending
                                        this.logging.logInfo(s"Modify primary keys event of table “$mysqlTable” was detected, primary key after change is ${keys.mkString(",")}")
                                        val regex = s".+$rowKeyWithEmptyPrimaryKeys"
                                        val filterList = new FilterList()
                                        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex)))
                                        val hBaseRDD = this.hBaseContext.hbaseRDD(TableName.valueOf(hTable), new Scan().setFilter(filterList)).map(_._2)
                                        this.hBaseContext.bulkPut[Result](hBaseRDD, TableName.valueOf(hTable), result => {
                                            val cells = result.listCells()
                                            val primaryKeyValues = keys.map(key => cells.find(cell => Bytes.toString(CellUtil.cloneQualifier(cell)) == key).orNull)
                                              .map(cell => Bytes.toString(CellUtil.cloneValue(cell)))
                                            val rowKey = himma.generateRowKey(database, sourceTable, primaryKeyValues)
                                            val put = new Put(Bytes.toBytes(rowKey))
                                            cells.foreach(cell => put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell)))
                                            put
                                        })
                                        this.hBaseContext.bulkDelete[Result](hBaseRDD, TableName.valueOf(hTable), result => new Delete(result.getRow), HBASE_BATCH_SIZE.intValue)
                                    case sub =>
                                }
                            } else {
                                this.logging.logWarning(s"Only drop primary key event of table $mysqlTable was detected, mapping of this table will be removed, synchronization of this table will be stopped, data in destination of this table will be deleted, if you want to restore this, add some primary keys to this table then restart himma")
                                mappingRecorder.remove(himma.generateMappingKey(database, mysqlTable))
                                this.truncate(database, sourceTable, hTable)
                            }
                        case addColumnRegex1(column, description) =>
                            //添加字段的Alter事件无需处理，因为即使添加的字段有默认值，也不会更新到已有的所有记录中
                            this.logging.logInfo(s"Add column “$column” to table “$mysqlTable”, description is “$description”, this type of alter isn't necessary to be synchronized")
                        case addColumnRegex2(column, description) =>
                            //添加字段的Alter事件无需处理，因为即使添加的字段有默认值，也不会更新到已有的所有记录中
                            this.logging.logInfo(s"Add column “$column” to table “$mysqlTable”, description is “$description”, this type of alter isn't necessary to be synchronized")
                        case changeColumnRegex1(before, after, description) => this.synchronizeChangeColumn(hTable, database, sourceTable, before, after, description)
                        case changeColumnRegex2(before, after, description) => this.synchronizeChangeColumn(hTable, database, sourceTable, before, after, description)
                        case dropColumnRegex1(column) => this.synchronizeDropColumn(hTable, database, sourceTable, column)
                        case dropColumnRegex2(column) => this.synchronizeDropColumn(hTable, database, sourceTable, column)
                        case modifyColumnRegex1(column, description) =>
                            //修改字段属性的Alter事件无需处理，修改类型也好，默认值也好，都不会产生数据的变化
                            this.logging.logInfo(s"Modify column “$column” of table “$mysqlTable”, description is “$description”, this type of alter isn't necessary to be synchronized")
                        case modifyColumnRegex2(column, description) =>
                            //修改字段属性的Alter事件无需处理，修改类型也好，默认值也好，都不会产生数据的变化
                            this.logging.logInfo(s"Modify column “$column” of table “$mysqlTable”, description is “$description”, this type of alter isn't necessary to be synchronized")
                        case sub => throw new UnsupportedOperationException(s"lack of code to deal with sub-statement “$sub” and whole ddl is “$ddl ”")
                    }
                case _ => throw new UnsupportedOperationException(s"lack of code to deal with ddl “$ddl”")
            }
        }
        if (result.isSuccess) {
            val content =
                s"""Himma监控到位于${MYSQL_HOST}:${MYSQL_PORT}的表$database.${sourceTable}发生结构修改，DDL如下:
                   |${e.ddl}
                   |若需要修改Hive对应业务表结构，请先联系Himma负责人进行him_all和him_inc库内相关表的结构修改""".stripMargin
            this.logging.logInfo(content)
            alerter.alert("【Himma】表结构修改提醒", content, null)
        } else
            throw result.failed.get
    }

    /**
     * 删除字段的Alter事件，直接删除字段即可
     *
     * @param hTable      HBase表名称
     * @param database    mysql数据库名称
     * @param sourceTable mysql数据表名称
     * @param column      被删除的mysql字段名称
     */
    private def synchronizeDropColumn(hTable: String, database: String, sourceTable: String, column: String) = {
        this.logging.logInfo(s"Drop column “$column” in table “$sourceTable”")
        val rowkeyWithEmptyPrimaryKeys = himma.generateRowKey(database, sourceTable, Array())
        val regex = s".+$rowkeyWithEmptyPrimaryKeys"
        val filterList = new FilterList()
        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex)))
        val hBaseRDD = this.hBaseContext.hbaseRDD(TableName.valueOf(hTable), new Scan().setFilter(filterList))
          .map(_._2)
          .filter(_.listCells().map(CellUtil.cloneQualifier).map(Bytes.toString).contains(column))
        this.hBaseContext.bulkDelete[Result](hBaseRDD, TableName.valueOf(hTable), result => {
            val delete = new Delete(result.getRow)
            result.listCells().filter(cell => Bytes.toString(CellUtil.cloneQualifier(cell)) == column)
              .foreach(cell => delete.addColumns(CellUtil.cloneFamily(cell), Bytes.toBytes(column)))
            delete
        }, HBASE_BATCH_SIZE.intValue)
    }

    /**
     * 同步重命名字段的Alter事件
     * 读取旧字段数据后删除旧字段数据，将读取到的数据更新发生重命名的字段值后写入
     *
     * @param hTable      HBase表名称
     * @param database    mysql数据库名称
     * @param sourceTable mysql数据表名称
     * @param before      修改前的字段名称
     * @param after       修改后的字段名称
     */
    private def synchronizeChangeColumn(hTable: String, database: String, sourceTable: String, before: String, after: String, description: String) = {
        this.logging.logInfo(s"Rename column of table “$sourceTable” from “$before” to “$after”, description is “$description”")
        val regex = himma.generateRowKeyRegexWithoutPrimaryKeys(database, sourceTable)
        val filterList = new FilterList()
        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex)))
        val hBaseRDD = this.hBaseContext.hbaseRDD(TableName.valueOf(hTable), new Scan().setFilter(filterList))
          .map(_._2)
          .filter(_.listCells().map(CellUtil.cloneQualifier).map(Bytes.toString).contains(before))
        this.hBaseContext.bulkPut[Result](hBaseRDD, TableName.valueOf(hTable), result => {
            val put = new Put(result.getRow)
            result.listCells().filter(cell => Bytes.toString(CellUtil.cloneQualifier(cell)) == before)
              .foreach(cell => put.addColumn(CellUtil.cloneFamily(cell), Bytes.toBytes(after.toLowerCase), CellUtil.cloneValue(cell)))
            put
        })
        this.hBaseContext.bulkDelete[Result](hBaseRDD, TableName.valueOf(hTable), result => {
            val delete = new Delete(result.getRow)
            result.listCells().filter(cell => Bytes.toString(CellUtil.cloneQualifier(cell)) == before)
              .foreach(cell => delete.addColumns(CellUtil.cloneFamily(cell), Bytes.toBytes(before)))
            delete
        }, HBASE_BATCH_SIZE.intValue)
    }

    /**
     * 同步重命名表事件
     * 表名发生修改：读取旧数据后删除旧数据，将读取到的数据更新rowKey后写入，修改映射关系
     *
     * @param hTable      HBase表名称
     * @param database    MySQL数据库名称
     * @param sourceTable MySQL数据库表名称
     * @param before      同sourceTable
     * @param after       sourceTable修改后的名字
     */
    private def synchronizeRenameTable(hTable: String, database: String, sourceTable: String, before: String, after: String) = {
        val regex = ".+" + himma.generateRowKey(database, sourceTable, Array())
        val filterList = new FilterList()
        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex)))
        val hBaseRDD = this.hBaseContext.hbaseRDD(TableName.valueOf(hTable), new Scan().setFilter(filterList)).map(_._2)
        val beforeMappingKey = himma.generateMappingKey(database, before)
        val afterMappingKey = himma.generateMappingKey(database, after)
        this.hBaseContext.bulkPut[Result](hBaseRDD, TableName.valueOf(hTable), result => {
            val beforeKey = Bytes.toString(result.getRow)
            val afterKey = beforeKey.replace(beforeMappingKey, afterMappingKey)
            val put = new Put(Bytes.toBytes(afterKey))
            result.listCells().foreach(cell => put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell)))
            put
        })
        this.hBaseContext.bulkDelete[Result](hBaseRDD, TableName.valueOf(hTable), result => new Delete(result.getRow), HBASE_BATCH_SIZE.intValue)
        mappingRecorder.rename(beforeMappingKey, afterMappingKey)
        this.logging.logInfo("Rename table event was detected, data and mapping was automatically modified by org.sa.himma, please make corresponding change to configuration file before next restart of org.sa.himma")
    }

    /**
     * 新建的表如果是需要同步的表，需要在建表前通知到大数据平台的人，在destination中手动增加一条映射关系，加完关系后再建表
     */
    def synchronizeCreate(): Unit = mappingRecorder.refresh()

    /**
     * 同步删除表事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeDrop(e: HimmaEntry, hTable: String, family: String) = {
        val database = e.database
        val sourceTable = e.table
        this.truncate(database, sourceTable, hTable)
        //mappingRecorder.getmappingRecorder.remove(himma.generateMappingKey(database, sourceTable))
    }

    /**
     * 从Hbase表中删除来自指定MySQL实例指定数据库中表的数据
     *
     * @param database    MySQL数据库名称
     * @param sourceTable 源表名称
     * @param hTable      Hbase表名称
     */
    def truncate(database: String, sourceTable: String, hTable: String): Unit = {
        val regex = himma.generateRowKeyRegexWithoutPrimaryKeys(database, sourceTable)
        val filterList = new FilterList()
        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex)))
        val hBaseRDD = this.hBaseContext.hbaseRDD(TableName.valueOf(hTable), new Scan().setFilter(filterList)).map(_._2)
        this.hBaseContext.bulkDelete[Result](hBaseRDD, TableName.valueOf(hTable), result => new Delete(result.getRow), HBASE_BATCH_SIZE.intValue)
    }

    /**
     * 同步截断事件
     *
     * @param e      解析后的MySQL Canal Entry
     * @param hTable 目标表名称
     * @param family 唯一列族名称
     */
    def synchronizeTruncate(e: HimmaEntry, hTable: String, family: String): Unit = {
        val database = e.database
        val sourceTable = e.table
        this.truncate(database, sourceTable, hTable)
    }

}
