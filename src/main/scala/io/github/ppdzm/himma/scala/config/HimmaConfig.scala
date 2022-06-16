package io.github.ppdzm.himma.scala.config

import io.github.ppdzm.utils.database.pool.mysql.MySQLHandlerPool
import io.github.ppdzm.utils.universal.config.{Config, ConfigItem}
import io.github.ppdzm.utils.universal.feature.Pool

/**
 * Created by Stuart Alex on 2017/2/9.
 */
class HimmaConfig(val config: Config) {
    /**
     * 启用Canal HA模式
     */
    lazy val CANAL_CLUSTER_ENABLED: ConfigItem = new ConfigItem(config, "canal.cluster.enabled", false)
    /**
     * Canal地址
     */
    lazy val CANAL_ADDRESS: ConfigItem = new ConfigItem(config, "canal.server.address")
    /**
     * Canal端口
     */
    lazy val CANAL_PORT: ConfigItem = new ConfigItem(config, "canal.server.port")
    /**
     * MySQL实例对应的Canal实例名称
     */
    lazy val CANAL_INSTANCE: ConfigItem = new ConfigItem(config, "canal.instance.name")
    /**
     * 配置项分隔符
     */
    lazy val CONFIG_SEPARATOR: ConfigItem = new ConfigItem(config, "himma.config.separator")
    /**
     * Himma实例名称
     */
    lazy val HIMMA_INSTANCE: ConfigItem = new ConfigItem(config, "himma.instance.name")
    /**
     * 告警器类型
     */
    lazy val HIMMA_ALERTER_TYPE: ConfigItem = new ConfigItem(config, "himma.alerter.type")
    /**
     * 是否检查Himma配置
     */
    lazy val HIMMA_CONFIG_CHECK: ConfigItem = new ConfigItem(config, "himma.config.check", false)
    /**
     * 是否检查Canal配置
     */
    lazy val HIMMA_CANAL_CONFIG_CHECK: ConfigItem = new ConfigItem(config, "himma.canal.config.check", false)
    /**
     * 是否检查MySQL配置
     */
    lazy val HIMMA_MYSQL_CONFIG_CHECK: ConfigItem = new ConfigItem(config, "himma.mysql.config.check", false)
    /**
     * 启用存量数据初始化
     */
    lazy val HIMMA_INIT_ENABLED: ConfigItem = new ConfigItem(config, "himma.init.enabled", false)
    /**
     * 启用增量数据同步
     */
    lazy val HIMMA_SYNC_ENABLED: ConfigItem = new ConfigItem(config, "himma.sync.enabled", false)
    /**
     * 日志获取批次大小
     */
    lazy val HIMMA_SYNC_BATCH_SIZE: ConfigItem = new ConfigItem(config, "himma.sync.batch.size", 1000)
    /**
     * 日志获取时间间隔
     */
    lazy val HIMMA_SYNC_INTERVAL: ConfigItem = new ConfigItem(config, "himma.sync.interval", 1)
    /**
     * 日志筛选正则
     */
    lazy val HIMMA_SYNC_FILTER: ConfigItem = new ConfigItem(config, "himma.sync.filter", "")
    /**
     * 映射关系记录
     */
    lazy val HIMMA_MAPPING_RECORDER: ConfigItem = new ConfigItem(config, "himma.mapping.recorder", "hbase")
    /**
     * 进度一致记录，类似barrier
     */
    lazy val HIMMA_SIGNAL_RECORDER: ConfigItem = new ConfigItem(config, "himma.signal.recorder", "hbase")
    /**
     * 延迟检测时间间隔，默认为1800秒
     */
    lazy val HIMMA_DELAY_CHECK_INTERVAL: ConfigItem = new ConfigItem(config, "himma.delay.check.interval", 1800000)
    /**
     * 最大允许的延迟时长，默认为1800秒
     */
    lazy val HIMMA_DELAY_THRESHOLD: ConfigItem = new ConfigItem(config, "himma.delay.threshold", 1800000)
    /**
     * 忽略延迟提醒的时间段，默认为0点-4点
     */
    lazy val HIMMA_DELAY_IGNORED_HOURS: ConfigItem = new ConfigItem(config, "himma.delay.ignored.hours", "0,1,2,3,4")
    /**
     * 是否启用无日志超限告警
     */
    lazy val HIMMA_SLEEPING_ALERT_ENABLED: ConfigItem = new ConfigItem(config, "himma.sleeping.alert.enabled", true)
    /**
     * 最大允许无日志的批次数，默认为43200，约12小时
     */
    lazy val HIMMA_SLEEPING_BATCH_THRESHOLD: ConfigItem = new ConfigItem(config, "himma.sleeping.batch.threshold", 43200)
    /**
     * MySQL host
     */
    lazy val MYSQL_HOST: ConfigItem = new ConfigItem(config, "himma.mysql.host")
    /**
     * MySQL port
     */
    lazy val MYSQL_PORT: ConfigItem = new ConfigItem(config, "himma.mysql.port", 3306)
    /**
     * MySQL username
     */
    lazy val MYSQL_USERNAME: ConfigItem = new ConfigItem(config, "himma.mysql.username")
    /**
     * MySQL password
     */
    lazy val MYSQL_PASSWORD: ConfigItem = new ConfigItem(config, "himma.mysql.password")
    /**
     * MySQL databases
     */
    lazy val MYSQL_DATABASES: ConfigItem = new ConfigItem(config, "himma.mysql.databases")
    /**
     * Regexp use to filter MySQL databases
     */
    lazy val MYSQL_DATABASES_REGEXP: ConfigItem = new ConfigItem(config, "himma.mysql.databases.regexp")
    /**
     * MySQL读取批次大小
     */
    lazy val MYSQL_READ_BATCH_SIZE: ConfigItem = new ConfigItem(config, "himma.mysql.read.batchSize", 1000)
    /**
     * 多字段主键生成RowKey时的连接符
     */
    lazy val HIMMA_KEY_CONNECTOR: ConfigItem = new ConfigItem(config, "himma.key.connector")
    /**
     * 是否启用MYSQL作为目标系统
     */
    lazy val MYSQL_SINK_ENABLED: ConfigItem = new ConfigItem(config, "himma.sink.mysql.enabled", false)
    /**
     * 是否启用MYSQL作为目标系统
     */
    lazy val KAFKA_SINK_ENABLED: ConfigItem = new ConfigItem(config, "himma.sink.kafka.enabled", false)
    /**
     * Kafka Brokers
     */
    lazy val KAFKA_BROKERS: ConfigItem = new ConfigItem(config, "kafka.brokers")
    /**
     * 是否启用HBase作为目标系统
     */
    lazy val HBASE_SINK_ENABLED: ConfigItem = new ConfigItem(config, "himma.sink.hbase.enabled", false)
    /**
     * HBase是否启用Kerberos
     */
    lazy val HBASE_SINK_KERBEROS_ENABLED: ConfigItem = new ConfigItem(config, "himma.sink.hbase.kerberos.enabled", false)
    /**
     * 同步目标命名空间
     */
    lazy val HBASE_NAMESPACE: ConfigItem = new ConfigItem(config, "himma.sink.hbase.namespace")
    /**
     * HBase Column Family
     */
    lazy val HBASE_COLUMN_FAMILY: ConfigItem = new ConfigItem(config, "himma.sink.hbase.columnFamily", "cf")
    /**
     * HBase Start Key
     */
    lazy val HBASE_START_KEY: ConfigItem = new ConfigItem(config, "himma.sink.hbase.startKey", "0")
    /**
     * HBase End Key
     */
    lazy val HBASE_END_KEY: ConfigItem = new ConfigItem(config, "himma.sink.hbase.endKey", "9")
    /**
     * HBase Region Number
     */
    lazy val HBASE_REGION_NUMBER: ConfigItem = new ConfigItem(config, "himma.sink.hbase.regionNumber", 11)
    /**
     * HBase写入批次大小
     */
    lazy val HBASE_BATCH_SIZE: ConfigItem = new ConfigItem(config, "himma.sink.hbase.batchSize", 1000)
    /**
     * reverse主键，大部分情况下，主键为id自增列，翻转可使row key在HBase中分布均匀
     */
    lazy val HBASE_ROW_KEY_REVERSE_PRIMARY_KEYS: ConfigItem = new ConfigItem(config, "himma.sink.hbase.primaryKeys.reverse", true)
    /**
     * 切割频率
     */
    lazy val HBASE_SWITCH_FREQUENCY: ConfigItem = new ConfigItem(config, "himma.sink.hbase.switch.frequency", "daily")
    /**
     * 需快照的表
     */
    lazy val HBASE_SNAPSHOT_TABLES: ConfigItem = new ConfigItem(config, "himma.sink.hbase.snapshot.tables", "")
    /**
     * 需增量的表
     */
    lazy val HBASE_INCREMENT_TABLES: ConfigItem = new ConfigItem(config, "himma.sink.hbase.increment.tables", "")
    /**
     */
    lazy val MAPPING_HBASE_STORAGE: ConfigItem = new ConfigItem(config, "himma.mapping.storage.hbase.table_name")
    /**
     * 映射关系存储
     */
    lazy val MAPPING_HBASE_QUALIFIER: ConfigItem = new ConfigItem(config, "himma.mapping.storage.hbase.qualifier", "value")
    /**
     * 快照信号值存储
     */
    lazy val SIGNAL_HBASE_STORAGE: ConfigItem = new ConfigItem(config, "himma.signal.storage.hbase.table_name")
    /**
     * 完备信号值，该数值表示有多少个himma实例是一组进程
     */
    lazy val SIGNAL_READY_NUMBER: ConfigItem = new ConfigItem(config, "himma.signal.readyNumber", 1)
    /**
     * 分组标识
     */
    lazy val SIGNAL_IDENTITY: ConfigItem = new ConfigItem(config, "himma.signal.identity")
    /**
     * 最终状态列名称
     */
    lazy val FINAL_STATUS_COLUMN_NAME: ConfigItem = new ConfigItem(config, "himma.final.status.column.name")
    lazy val ZOOKEEPER_CONNECTION: ConfigItem = new ConfigItem(config, "zookeeper.connection")
    lazy val ZOOKEEPER_PORT: ConfigItem = new ConfigItem(config, "zookeeper.port", 2181)
    lazy val ZOOKEEPER_QUORUM: ConfigItem = new ConfigItem(config, "zookeeper.quorum")
    /**
     * 初始化时分页查询的大小
     */
    protected val initializationBatchSize: ConfigItem = new ConfigItem(config, "himma.initialization.batch.size", 100000)
    /**
     * 实时同步主表Hive数据库
     */
    protected val mainHiveDatabase: ConfigItem = new ConfigItem(config, "himma.hive.database.main")
    /**
     * 增量表Hive数据库
     */
    protected val incrementalHiveDatabase: ConfigItem = new ConfigItem(config, "himma.hive.database.incremental")
    /**
     * 启用实时计算日志转发功能
     */
    protected val rtcEnabled: ConfigItem = new ConfigItem(config, "himma.rtc.enabled", false)
    /**
     * 确保日志重合处完全被消费掉，延迟转发
     */
    protected val delayTransmit: ConfigItem = new ConfigItem(config, "himma.delay.transmit", 300000)

    /**
     * 获取数据库列表
     *
     * @return
     */
    def getDatabases: List[String] = {
        if (MYSQL_DATABASES.isDefined)
            MYSQL_DATABASES.arrayValue(CONFIG_SEPARATOR.stringValue).toList
        else {
            Pool.borrow(MySQLHandlerPool(getMySQLUrl("information_schema"), null)) {
                handler => handler.listTables("information_schema", MYSQL_DATABASES_REGEXP.stringValue)
            }
        }
    }

    /**
     * 获取MySQL数据库所在实例URL
     *
     * @param databaseAlias 数据库别名
     * @return
     */
    def getMySQLUrl(databaseAlias: String): String = {
        val database = getDatabaseName(databaseAlias)
        // 从配置中获取可以访问当前数据库的用户名（可设定统一由canal使用的用户名及密码）
        val username = config.getProperty(s"himma.mysql.database.$database.username", MYSQL_USERNAME.stringValue)
        // 从配置中获取可以访问当前数据库的密码（可设定统一由canal使用的用户名及密码）
        val password = config.getProperty(s"himma.mysql..database.$database.password", MYSQL_PASSWORD.stringValue)
        s"jdbc:mysql://${MYSQL_HOST.stringValue}:${MYSQL_PORT.intValue}/$database?user=$username&password=$password"
    }

    /**
     * 获取数据库名称
     *
     * @param databaseAlias 数据库别名
     * @return
     */
    def getDatabaseName(databaseAlias: String): String = {
        config.getProperty(s"himma.mysql.database.$databaseAlias.name", databaseAlias)
    }

    /**
     * 获取表分组别名列表
     *
     * @param databaseAlias 数据库别名
     * @return
     */
    def getTableGroupAliasList(databaseAlias: String): Array[String] = {
        config.newConfigItem(s"himma.mysql.database.$databaseAlias.tables").arrayValue(CONFIG_SEPARATOR.stringValue)
    }

    /**
     * 获取表列表
     *
     * @param databaseAlias   数据库别名
     * @param tableGroupAlias 表分组别名
     * @return
     */
    def getTables(databaseAlias: String, tableGroupAlias: String): List[String] = {
        Pool.borrow(MySQLHandlerPool(getMySQLUrl(databaseAlias), null)) {
            handler => handler.listTables(getDatabaseName(databaseAlias), getRegexp(databaseAlias, tableGroupAlias))
        }
    }

    /**
     * 从配置中获取分表或单表的正则表达式
     *
     * @param databaseAlias   数据库别名或简称（全称）
     * @param tableGroupAlias 单表/表分组别名或简称
     * @return
     */
    def getRegexp(databaseAlias: String, tableGroupAlias: String): String = {
        config.getProperty(s"himma.mysql.database.$databaseAlias.table.$tableGroupAlias.regexp", tableGroupAlias)
    }

    /**
     * 获取字段列表
     *
     * @param databaseAlias   数据库别名
     * @param tableGroupAlias 表分组别名
     * @return
     */
    def getFields(databaseAlias: String, tableGroupAlias: String): Array[String] = {
        config.newConfigItem(s"himma.mysql.database$databaseAlias.table.$tableGroupAlias.fields", "").arrayValue(CONFIG_SEPARATOR.stringValue)
    }

    /**
     * 获取配置中该表是否重新初始化的定义
     *
     * @param databaseAlias   数据库别名或简称（全称）
     * @param tableGroupAlias 单表/表分组别名或简称
     * @return
     */
    def getReInitialize(databaseAlias: String, tableGroupAlias: String): Boolean = {
        config.newConfigItem(s"himma.mysql.database.$databaseAlias.table.$tableGroupAlias.reinitialize", false).booleanValue
    }

    /**
     * 获取用于进行分页查询的字段
     *
     * @param databaseAlias   数据库别名或简称（全称）
     * @param tableGroupAlias 单表/表分组别名或简称
     * @return
     */
    def getPagingColumn(databaseAlias: String, tableGroupAlias: String): String = {
        config.getProperty(s"himma.mysql.database.$databaseAlias.table.$tableGroupAlias.pagingColumn", "id")
    }

    /**
     * 获取HBase表名
     *
     * @param databaseAlias   数据库别名
     * @param tableGroupAlias 表分组别名
     * @return
     */
    def getHBaseTableName(databaseAlias: String, tableGroupAlias: String): String = {
        val name = config.getProperty(s"himma.mysql.database.$databaseAlias.table.$tableGroupAlias.sink.hbase.table_name")
        s"${HBASE_NAMESPACE.stringValue}:$name"
    }

    /**
     * 获取唯一键
     *
     * @param databaseAlias 数据库别名
     * @param tableName     表名
     * @return
     */
    def getUniqueKey(databaseAlias: String, tableName: String): String = {
        val database = config.getProperty(s"himma.mysql.database.$databaseAlias.name", databaseAlias)
        s"$MYSQL_HOST:$MYSQL_PORT/$database.$tableName"
    }

    /**
     * 获取逻辑主键，实际选用主键时，可能不使用数据库的物理主键，而使用逻辑主键
     *
     * @param databaseAlias   数据库别名
     * @param tableGroupAlias 数据表分组的别名
     * @param realTable       实际的表
     * @return
     */
    def getLogicalPrimaryKeys(databaseAlias: String, tableGroupAlias: String, realTable: String): Array[String] = {
        val logicalPrimaryKeys = config.newConfigItem(s"himma.mysql.database.$databaseAlias.table.$tableGroupAlias.logicalPrimaryKeys")
        if (logicalPrimaryKeys.isDefined)
            logicalPrimaryKeys.arrayValue(CONFIG_SEPARATOR.stringValue)
        else {
            getPhysicalPrimaryKeys(databaseAlias, realTable)
        }
    }

    /**
     * 获取物理主键
     *
     * @param databaseAlias 数据库别名
     * @param realTable     实际的表
     * @return
     */
    def getPhysicalPrimaryKeys(databaseAlias: String, realTable: String): Array[String] = {
        val database = getDatabaseName(databaseAlias)
        val url = getMySQLUrl(databaseAlias)
        val sql = s"select column_name from information_schema.columns where table_schema='$database' and table_name='$realTable' and column_key='PRI'"
        Pool.borrow(MySQLHandlerPool(url, null)) {
            handler => handler.query(sql).singleColumnList(0, null2Empty = true).ascending.toArray
        }
    }

}
