package io.github.ppdzm.himma.scala.synchronizer

import io.github.ppdzm.himma.scala.component.Himma

/**
 * Created by Stuart Alex on 2017/12/19.
 * DDL处理器
 */
class DDLHandler(himma: Himma) {

    import himma._
    import himma.himmaConfig._

    /**
     * 从HBase表中删除来自指定MySQL实例指定数据库中表的数据
     *
     * @param database    MySQL数据库名称
     * @param sourceTable 源表名称
     * @param hTable      Hbase表名称
     */
    def truncate(database: String, sourceTable: String, hTable: String): Unit = {
        val regexp = himma.generateRowKeyRegexWithoutPrimaryKeys(database, sourceTable)
        hbaseHandler.bulkDelete(hTable, regexp, HBASE_BATCH_SIZE.intValue)
    }

}
