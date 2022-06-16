package io.github.ppdzm.himma.scala.initializer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.utils.database.pool.mysql.MySQLHandlerPool
import io.github.ppdzm.utils.universal.base.Logging
import io.github.ppdzm.utils.universal.feature.Pool
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

case class HBaseInitializer(himma: Himma, hBaseTableName: String) extends Initializer {

    import himma._
    import himma.himmaConfig._

    private val logging = new Logging(getClass)

    override def initialize(url: String, database: String, table: String, primaryKeys: Array[String], fields: Array[String]): Unit = {
        assert(primaryKeys.nonEmpty, "primary key column names are empty")
        this.logging.logInfo(s"Primary keys of table $table is ${primaryKeys.mkString(",")}")
        val data =
            Pool.borrow(MySQLHandlerPool(url, null)) {
                handler => handler.query(s"select * from $table")
            }
        var batchCount = 0
        var rowCount = 0
        val expectedFields = getExpectedFields(data, fields)
        val putListBuffer = ListBuffer[Put]()
        while (data.next()) {
            val primaryKeyValues = primaryKeys.map(data.getString)
            val rowKey = himma.generateRowKey(database, table, primaryKeyValues)
            val put = new Put(Bytes.toBytes(rowKey))
            expectedFields.map(f => (f, data.getString(f)))
              .foreach {
                  e => put.addColumn(HBASE_COLUMN_FAMILY.bytesValue, Bytes.toBytes(e._1), Bytes.toBytes(e._2))
              }
            putListBuffer += put
            if (rowCount % HBASE_BATCH_SIZE.intValue == 0) {
                batchCount += 1
                rowCount += putListBuffer.length
                hbaseHandler.bulkPut(hBaseTableName, putListBuffer.toList, HBASE_BATCH_SIZE.intValue)
                putListBuffer.clear()
                this.logging.logInfo(s"Batch initialization finished on batch $batchCount")
            }
        }
        if (putListBuffer.nonEmpty) {
            batchCount += 1
            rowCount += putListBuffer.length
            hbaseHandler.bulkPut(hBaseTableName, putListBuffer.toList, HBASE_BATCH_SIZE.intValue)
            putListBuffer.clear()
            this.logging.logInfo(s"Batch initialization finished on batch $batchCount")
        }
        if (rowCount == 0) {
            this.logging.logWarning(s"Table $table is empty")
        } else {
            this.logging.logInfo(s"Batch initialization finished with $rowCount rows")
        }
    }

    private def getExpectedFields(data: ResultSet, fields: Array[String]): Array[String] = {
        val metadata = data.getMetaData
        val columnCount = metadata.getColumnCount
        if (fields.nonEmpty)
            fields
        else
            (1 to columnCount).map(data.getMetaData.getColumnName).toArray
    }


}
