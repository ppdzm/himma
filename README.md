# 异构数据库实时同步程序部署步骤  
## Canal Server部署
- 修改MySQL数据库配置my.cnf
```
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server_id= #此项自定义，避免与上述Canal Server端配置中的slaveId重复
```
- 在需要同步的MySQL实例上创建用户canal，并赋予权限
```
create user canal;
grant select, replication slave, replication client, super on *.* to 'canal'@'%' identified by 'canal';
flush privileges;
```
- 下载并解压canal  
见Himma部署  
- 根据具体需求进行实例配置，假定实例命名为my-instance（可配置多个实例）
```
cp -r canal/conf/example canal/conf/my-instance  
vim canal/conf/my-instance/instance.properties
#################################################  
##MySQL server id，要确保在整个canal server的所有canal instance中唯一
canal.instance.mysql.slaveId = 1234
##MySQL address:port，根据实际值进行修改
canal.instance.master.address = 127.0.0.1:3306  
canal.instance.master.journal.name =   
canal.instance.master.position =   
canal.instance.master.timestamp =   
#canal.instance.standby.address =   
#canal.instance.standby.journal.name =  
#canal.instance.standby.position =   
#canal.instance.standby.timestamp =   
canal.instance.dbUsername =   
canal.instance.dbPassword =   
canal.instance.defaultDatabaseName =  
##如果一个数据库中的表含有2种级以上的编码，需要将该数据库拆开进行同步
canal.instance.connectionCharset = UTF-8  
##要订阅日志的表的筛选正则表达式
canal.instance.filter.regex = .*\\..*  
#################################################  
```
- 启动Server  
canal/bin/start.sh  

## Himma架构  
[Himma具体架构](http://note.youdao.com/share/?id=c363482faf59547e1a9781bb6e45b5c2&type=note#/)  
## Himma部署
- 打包  
- 上传tar包到服务器  
- 解压获取canal和himma配置、运行脚本等  
- 增加新配置，编写新运行脚本  
```shell
cd himma  
cp -r conf/example conf/new-conf  
vim conf/new-conf/application.properties  
cp -r bin/example/ bin/new-conf  
vim bin/new-conf/start.sh  
```
- 运行新应用  
bin/master.sh new-conf
- application.properties配置文件详解  

配置项|说明|默认值
---|---|---
canal.server.address|Canal Server地址|
canal.server.port|Canal Server端口号|11111
mysql.default.username|Canal Instance连接MySQL使用的默认用户名|canal
mysql.default.password|Canal Instance连接MySQL使用的默认密码|canal
mysql.key.connector|多字段主键连接符|+
himma.delay.ignored.hours|忽略延迟告警的小时|0,1,2,3,4
himma.delay.check.interval|延迟检查时间间隔(millisecond)|1800000
himma.delay.threshold|最大允许延迟时长(minutes)|30
himma.canal.check|是否对Canal的日志有效性进行检查|false
himma.config.check|是否对配置文件的有效性进行检查|true
himma.syncr.interval|程序消费日志的时间间隔|1000
himma.destination.namespace|HBase目标命名空间|
himma.destination.family|MySQL所有字段将写入HBase一个列族下，该配置项是这个列族的名称|data
himma.enabled|是否在初始化后进行实时同步|false
himma.aggregate.enabled|是否对已初始化的表进行补充聚合操作, 谨慎使用, 异常中断目前需要手动重置聚合表|false
himma.mapping.storage|存储MySQL源表——HBase目标表映射关系的表|himma_synchronization_mapping
himma.signal.storage|存储多进程协作信号值的表名称|himma_ready_signal
himma.signal.number|进程数目|
himma.signal.identity|进程组唯一标识|
himma.properties.separator|配置项的分隔符|,
himma.sleeping.alert.enabled|控制是否启用无日志超限告警|true
himma.sleeping.batch.max|最大允许的无日志周期(取日志批次数)，超过此值则产生告警|43200，大约12小时
himma.initialization.batch.size|初始化时的分页查询大小(行)|1000000
himma.synchronization.batch.size|同步时日志获取批次大小|1000
himma.mysql.check|是否对MySQL表的主键有效性进行检查|true
himma.mysql.instance|需要本程序消费的Canal Instance简称或名称|例如i1
himma.mysql.instance.$i.name|Canal Instance的名称，即canal conf下的文件夹名称|
himma.mysql.instance.$i.address|该Canal Instance订阅的MySQL实例地址|
himma.mysql.instance.$i.port|该Canal Instance订阅的MySQL实例端口|
himma.mysql.instance.$i.username|该Canal Instance订阅的MySQL用户名|canal，不设置则取默认值
himma.mysql.instance.$i.password|该Canal Instance订阅的MySQL密码|canal，不设置则取默认值
himma.mysql.instance.$i.databases|该Canal Instance订阅的MySQL实例中的数据库序列|例如s1,s2
himma.mysql.instance.$i.filter|用于该Canal Instance对MySQL日志进行筛选时使用的正则表达式|
himma.mysql.instance.$i.database.$d.name|数据库实际名称|
himma.mysql.instance.$i.database.$d.tables|该数据库下需同步的表序列|例如user,account,t
himma.mysql.instance.$i.database.$d.table.$t.regex|获取表使用的正则表达式|$t，不设置则取默认值
himma.mysql.instance.$i.database.$d.table.$t.pagingColumn|分页查询使用的分页字段|id
himma.mysql.instance.$i.database.$d.table.$t.destination|源表对应的HBase目标表名称|不设置则取默认值$t
himma.mysql.instance.$i.database.$d.table.$t.reinitialize|源表对应的HBase目标表名称|不设置则取默认值false
himma.mysql.instance.$i.database.$d.table.$t.version|HBase目标表data列族的版本数|不设置则取默认值1
himma.destination.$i.$t.incremental|指定是否每天为HBase目标表生成日增量表|不设置则取默认值false
himma.destination.$i.$t.snapshot|指定是否每天凌晨为HBase目标表生成快照|不设置则取默认值false
himma.destination.$i.$t.accumulation.suffix|累加器存储表相对于主表的后缀|不设置则取默认值accumulation
himma.destination.$i.$t.accumulation.aggregate.columns|进行累加聚合的参照列(可多个), 即group by的列|
himma.destination.$i.$t.accumulation.columns|进行累加聚合的列(可多个), 即sum的列|
himma.destination.$i.$t.count.suffix|计数器存储表相对于主表的后缀|不设置则取默认值count
himma.destination.$i.$t.count.aggregate.columns|进行计数聚合的参照列(可多个), 即group by的列|
hbase.master|HBase Master地址|
hdfs.ha|HDFS是否启用HA模式|false
hdfs.nameservice|HDFS Name Service名称|
hdfs.namenodes|HDFS Namenodes名称，以逗号分隔|
hdfs.namenode.address|HDFS Namenode地址，若未启用HA模式，需要设置此项|
hdfs.port|HDFS端口号|8020
zookeeper.quorum|Zookeeper地址序列，以逗号分隔|
zookeeper.port|Zookeeper端口号|2181
- 运行
    ```
    //使用--configuration-key value作为参数传入程序，可以更改配置文件中configuration-key配置项的值
    cd himma
    bin/start.sh <conf-dir> [other paramters of jar]
    ```
- 最后  
注意：himma.mysql.instance可以是简称，也可以是全称，即和canal conf下的子文件夹名称相同，而himma运行时的配置文件和运行脚本所在的文件夹最好也和canal conf名称一致，这样一对一更好理解，便于管理