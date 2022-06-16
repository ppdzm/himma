package io.github.ppdzm.himma.scala.initializer

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.utils.universal.base.Logging

/**
 * Created by Stuart Alex on 2016/11/29.
 */
case class StockDataInitializer(himma: Himma) {

    import himma._
    import himma.himmaConfig._

    private val logging = new Logging(getClass)

    /**
     * 处理存量数据
     *
     */
    def initialize(): Unit = {
        this.logging.logInfo(s"Begin initialization progress")
        // 获取需初始化的数据库（可能是别名）
        hbaseHandler.createNamespace(HBASE_NAMESPACE.stringValue)
        getDatabases.foreach {
            // 初始化每个数据库中的存量数据
            databaseAlias =>
                // 从配置中获取当前数据库中需要同步的表的定义（表名或表正则表达式）
                getTableGroupAliasList(databaseAlias).foreach {
                    tableGroupAlias =>
                        // 对每个表（或表匹配模式）进行存量数据的初始化
                        val mysqlTableNameList = getTables(databaseAlias, tableGroupAlias)
                        if (mysqlTableNameList.isEmpty)
                            this.logging.logInfo(s"Can't find any tables with databaseAlias “$databaseAlias” and tableGroupAlias “$tableGroupAlias”")
                        else {
                            this.logging.logInfo(s"Initializing data with databaseAlias “$databaseAlias” and tableGroupAlias “$tableGroupAlias” and found ${mysqlTableNameList.length} tables matches")
                            initialize2HBase(databaseAlias, tableGroupAlias, mysqlTableNameList)
                            brush2MySQL(databaseAlias, tableGroupAlias, mysqlTableNameList)
                        }
                }
        }
        this.logging.logInfo("Initialization progress finished")
    }

    private def initialize2HBase(databaseAlias: String, tableGroupAlias: String, mysqlTableNameList: List[String]): Unit = {
        if (HBASE_SINK_ENABLED.booleanValue) {
            val url = getMySQLUrl(databaseAlias)
            val database = getDatabaseName(databaseAlias)
            val fields = getFields(databaseAlias, tableGroupAlias)
            val reinitialize = getReInitialize(databaseAlias, tableGroupAlias)
            // 获取目标HBase表名称
            val hBaseTableName = getHBaseTableName(databaseAlias, tableGroupAlias)
            // 创建HBase目标表
            hbaseHandler.createTable(hBaseTableName, Array(HBASE_COLUMN_FAMILY.stringValue), HBASE_START_KEY.stringValue, HBASE_END_KEY.stringValue, HBASE_REGION_NUMBER.intValue)
            mysqlTableNameList.foreach {
                mysqlTableName =>
                    this.logging.logInfo(s"Start brush data from table $mysqlTableName into HBase")
                    val logicalPrimaryKeys = getLogicalPrimaryKeys(databaseAlias, tableGroupAlias, mysqlTableName)
                    val mappingKey = himma.generateMappingKey(database, mysqlTableName)
                    mappingRecorder.primaryKeysMapping += mappingKey -> logicalPrimaryKeys
                    val initializer = HBaseInitializer(himma, hBaseTableName)
                    // 如果源表——目标表的映射关系不存在
                    if (!mappingRecorder.exists(mappingKey)) {
                        initializer.initialize(url, database, mysqlTableName, logicalPrimaryKeys, fields)
                        // TODO 创建外部表
                        // 添加源和目标的映射关系
                        mappingRecorder.add(mappingKey, hBaseTableName)
                        this.logging.logInfo(s"Table $mysqlTableName brush finished")
                    } else if (reinitialize) {
                        // 目标表存在，源表——目标表的映射关系也存在，但配置中指定了该表需要重新初始化，则删除原有数据后重新加载
                        ddlSynchronizer.truncate(database, mysqlTableName, hBaseTableName)
                        initializer.initialize(url, database, mysqlTableName, logicalPrimaryKeys, fields)
                    } else {
                        this.logging.logInfo(s"Table $mysqlTableName already brushed into HBase, skip to next")
                    }
            }
        }
    }

    private def brush2MySQL(databaseAlias: String, tableGroupAlias: String, mysqlTableNameList: List[String]): Unit = {
        if (MYSQL_SINK_ENABLED.booleanValue) {
            //TODO
        }
    }

    /**
     * 对已初始化的表进行聚合运算
     *
     * @param instanceAddress MySQL实例地址
     * @param databaseName    数据库简称
     * @param url             MySQL连接字符串
     * @param source          源表
     * @param pagingColumn    分页查询字段
     * @param destination     目标表
     */
    @Deprecated
    private def temporary(instanceAddress: String, databaseName: String, url: String, source: String, pagingColumn: String, destination: String) = {
        //    val size = SparkSQL.mysql.size(url, source)
        //    val data = SparkSQL.mysql.df(url, source)
        //    val max = data.selectExpr(s"coalesce(max($pagingColumn),0) as max").collect().head.get(0).toString.toLong
        //    val min = data.selectExpr(s"coalesce(min($pagingColumn),0) as min").collect().head.get(0).toString.toLong
        //    if (max > 0 && min >= 0) {
        //      val span = max - min + 1
        //      if (size <= 100 * 1024 * 1024 || span < initializationBatchSize) {
        //        HimmaHBaseUtils.accumulate(data, instanceAddress, databaseName, source, destination, family)
        //        HimmaHBaseUtils.count(data, instanceAddress, databaseName, source, destination, family)
        //      } else {
        //        val number = if (span % initializationBatchSize == 0) span / initializationBatchSize else span / initializationBatchSize + 1
        //        (0 until number.toInt).foreach(cursor => {
        //          val lower = min + cursor * initializationBatchSize
        //          val upper = min + (cursor + 1) * initializationBatchSize
        //          HimmaHBaseUtils.accumulate(data.filter(s"$pagingColumn>=$lower and $pagingColumn<$upper"), instanceAddress, databaseName, source, destination, family)
        //          HimmaHBaseUtils.count(data.filter(s"$pagingColumn>=$lower and $pagingColumn<$upper"), instanceAddress, databaseName, source, destination, family)
        //        })
        //      }
        //    }
    }

}
