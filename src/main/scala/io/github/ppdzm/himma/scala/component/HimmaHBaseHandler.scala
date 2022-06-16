package io.github.ppdzm.himma.scala.component

import io.github.ppdzm.utils.hadoop.hbase.HBaseEnvironment
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.feature.LoanPattern
import io.github.ppdzm.utils.universal.implicits.ArrayConversions._
import io.github.ppdzm.utils.universal.implicits.BasicConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Row, functions}

import java.sql.{ResultSet, SQLException}
import scala.util.Try

/**
 * Created by Stuart Alex on 2016/11/14.
 */
class HimmaHBaseHandler(himma: Himma) extends HBaseEnvironment {

    import himma._
    import himma.himmaConfig._


    private lazy val hBaseContext = new HBaseContext(sparkSession.sparkContext, configuration)
    private lazy val logging = new Logging(getClass)
    override protected val zookeeperQuorum: String = ZOOKEEPER_QUORUM.stringValue()
    override protected val zookeeperPort: Int = ZOOKEEPER_PORT.intValue()

    /**
     *
     * 批量写入
     *
     * @param dataFrame       数据
     * @param instanceAddress ip地址
     * @param databaseName    数据库名称
     * @param source          表名称
     * @param keys            主键
     * @param hTable          目标表名称
     * @param family          唯一列族名称
     */
    def put(dataFrame: DataFrame, instanceAddress: String, databaseName: String, source: String, keys: Array[String], hTable: String, family: String) = {
        val result = Try {
            val schema = dataFrame.schema
            this.logging.logInfo(s"Start write data of table $source into HBaseTable $hTable")
            this.hBaseContext.bulkPut[Row](dataFrame.rdd, TableName.valueOf(hTable), row => {
                val primaryKeys = keys.map(key => row.get(row.fieldIndex(key)).toString)
                val rowKey = himma.generateRowKey(databaseName, source, primaryKeys)
                val put = new Put(Bytes.toBytes(rowKey))
                schema.filter(sf => row.get(row.fieldIndex(sf.name)) != null)
                  .foreach(sf => {
                      val value = row.get(row.fieldIndex(sf.name)).toString.replaceSpecialSymbol
                      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(sf.name.toLowerCase), Bytes.toBytes(value))
                  })
                put
            })
            this.logging.logInfo(s"Write data of table $source into HBaseTable $hTable finished")
        }
        handleException(result, hTable)
    }

    private def handleException(result: Try[Unit], hTable: String): Unit = {
        if (result.isFailure)
            result.failed.get match {
                case e: SQLException => this.logging.logError(e.getMessage, e)
                case t: Exception =>
                    this.logging.logError(s"Error occurred in initialization of table $hTable", t)
                    throw t
            }
    }

    /**
     *
     * 批量写入
     *
     * @param rs              数据
     * @param instanceAddress ip地址
     * @param databaseName    数据库名称
     * @param source          表名称
     * @param keys            主键
     * @param hTable          目标表名称
     * @param family          唯一列族名称
     */
    def put(rs: ResultSet, instanceAddress: String, databaseName: String, source: String, keys: Array[String], hTable: String, family: String): Unit = {
        val result = Try {
            this.logging.logInfo(s"Start write data of table $source into HBaseTable $hTable")
            val meta = rs.getMetaData
            LoanPattern.using(connection.getBufferedMutator(TableName.valueOf(hTable)))(bufferedMutator => {
                while (rs.next()) {
                    val primaryKeys = keys.map(key => rs.getObject(key).toString)
                    val rowKey = himma.generateRowKey(databaseName, source, primaryKeys)
                    val put = new Put(Bytes.toBytes(rowKey))
                    (1 to meta.getColumnCount).foreach(i => {
                        val o = rs.getObject(meta.getColumnName(i))
                        if (o.notNull) {
                            val value = o.toString.replaceSpecialSymbol
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(meta.getColumnName(i).toLowerCase), Bytes.toBytes(value))
                        }
                    })
                    bufferedMutator.mutate(put)
                }
                bufferedMutator.flush()
                bufferedMutator.close()
            })
            this.logging.logInfo(s"Write data of table $source into HBaseTable $hTable finished")
        }
        handleException(result, hTable)
    }

    /**
     * 生成累加表
     *
     * @param dataFrame       数据
     * @param instanceAddress ip地址
     * @param databaseName    数据库名称
     * @param source          表名称
     * @param hTable          目标表名称
     * @param family          唯一列族名称
     */
    @Deprecated
    def accumulate(dataFrame: DataFrame, instanceAddress: String, databaseName: String, source: String, hTable: String, family: String) = {
        val tableNameWithoutNamespace = hTable.replace(HBASE_NAMESPACE.stringValue + ":", "")
        val aggregateColumns =
            config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.aggregate.columns", "").arrayValue().map(_.trim).filter(_.notNullAndEmpty) //.ascending
        if (aggregateColumns.nonEmpty) {
            val suffix = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.suffix", "accumulation").stringValue()
            val tableName = TableName.valueOf(hTable + "_" + suffix)
            hbaseHandler.createTable(hTable + "_" + suffix, Array(family))
            val accumulationColumns = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.accumulation.columns").arrayValue().map(_.trim).filter(_.notNullAndEmpty).ascending
            LoanPattern.using(connection.getTable(tableName))(hBaseTable => {
                val data = dataFrame.groupBy(aggregateColumns.map(dataFrame(_)): _*)
                  .agg(functions.sum(accumulationColumns.head) as accumulationColumns.head, accumulationColumns.drop(1).map(column => functions.sum(column) as column): _*)
                  .selectExpr(aggregateColumns.++:(accumulationColumns): _*)
                data.collect().foreach(a => {
                    val rowKey = himma.generateRowKey(databaseName, source, aggregateColumns.map(key => a.get(a.fieldIndex(key)).toString))
                    accumulationColumns.foreach(column => {
                        hBaseTable.incrementColumnValue(rowKey.getBytes, family.getBytes, (column + "_accumulation").getBytes, a.get(a.fieldIndex(column)).toString.toLong)
                    })
                })
            })
        }
    }

    /**
     * 生成计数表
     *
     * @param dataFrame       数据
     * @param instanceAddress ip地址
     * @param databaseName    数据库名称
     * @param source          表名称
     * @param hTable          目标表名称
     * @param family          唯一列族名称
     */
    @Deprecated
    def count(dataFrame: DataFrame, instanceAddress: String, databaseName: String, source: String, hTable: String, family: String) = {
        val tableNameWithoutNamespace = hTable.replace(HIMMA_INSTANCE.stringValue + ":", "")
        val columns = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.count.aggregate.columns", "").arrayValue(",").map(_.trim).filter(_.notNullAndEmpty).ascending
        if (columns.nonEmpty) {
            val suffix = config.newConfigItem(s"himma.destination.${HIMMA_INSTANCE.stringValue}.$tableNameWithoutNamespace.count.suffix", "count").stringValue()
            val tableName = TableName.valueOf(hTable + "_" + suffix)
            hbaseHandler.createTable(hTable + "_" + suffix, Array(family))
            LoanPattern.using(connection.getTable(tableName))(hBaseTable => {
                val data = dataFrame.groupBy(columns.map(dataFrame(_)): _*)
                  .agg(functions.count(functions.lit(1)) as "count")
                  .selectExpr(columns :+ "count": _*)
                data.collect().foreach(a => {
                    val rowKey = himma.generateRowKey(databaseName, source, columns.map(key => a.get(a.fieldIndex(key)).toString))
                    hBaseTable.incrementColumnValue(rowKey.getBytes, family.getBytes, "count".getBytes, a.get(a.fieldIndex("count")).toString.toLong)
                })
            })
        }
    }
}