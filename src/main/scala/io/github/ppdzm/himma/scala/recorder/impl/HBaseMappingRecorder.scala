package io.github.ppdzm.himma.scala.recorder.impl

import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.himma.scala.recorder.MappingRecorder
import io.github.ppdzm.utils.hadoop.hbase.HBaseHandler
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
 * 记录源表——目标表映射关系
 * Created by Stuart Alex on 2017/1/19.
 */
private[recorder] case class HBaseMappingRecorder(himmaConfig: HimmaConfig) extends MappingRecorder {

    import himmaConfig._

    private lazy val mapping = mutable.Map[String, (String, Long)]()
    private lazy val hBaseHandler = new HBaseHandler(ZOOKEEPER_QUORUM.stringValue(), ZOOKEEPER_PORT.intValue())

    open()


    //
    //  /**
    //   * 获取一条映射关系的value
    //   *
    //   * @param key          key
    //   * @param defaultValue 默认值
    //   * @return
    //   */
    //  def getOrElse(key: String, defaultValue: String) = {
    //    if (this.mapping.contains(key))
    //      this.mapping(key)
    //    else
    //      defaultValue
    //  }
    //
    //  /**
    //   * 打印当前已存在的映射关系
    //   */
    //  def display() = this.mapping.foreach(pair => s"${pair._1} is mapping to ${pair._2}".prettyPrintln())

    /**
     * 初始化映射关系
     *
     * @return
     */
    override def open(): Unit = {
        hBaseHandler.createTable(MAPPING_HBASE_STORAGE.stringValue, Array(HBASE_COLUMN_FAMILY.stringValue))
        hBaseHandler.get(MAPPING_HBASE_STORAGE.stringValue)
          .map(r => {
              val rk = Bytes.toString(r.getRow)
              val value = Bytes.toString(r.getValue(HBASE_COLUMN_FAMILY.bytesValue, MAPPING_HBASE_QUALIFIER.bytesValue))
              val time = r.getColumnLatestCell(HBASE_COLUMN_FAMILY.bytesValue, MAPPING_HBASE_QUALIFIER.bytesValue).getTimestamp
              rk -> (value, time)
          })
          .filter(r => hBaseHandler.tableExists(r._2._1))
          .foreach(mapping += _)
    }

    override def exists(source: String): Boolean = {
        this.mapping.contains(source) || hBaseHandler.exists(MAPPING_HBASE_STORAGE.stringValue, source)
    }

    /**
     * 修改一条映射关系
     *
     * @param before 修改前的key
     * @param after  修改后的key
     */
    def rename(before: String, after: String): Unit = {
        val value = this.get(before)
        if (value != null) {
            this.remove(before)
            this.add(after, value._1)
        }
    }

    /**
     * 添加一条映射关系
     *
     * @param source      key
     * @param destination value
     */
    override def add(source: String, destination: String): Unit = {
        hBaseHandler.put(MAPPING_HBASE_STORAGE.stringValue, source, HBASE_COLUMN_FAMILY.stringValue, Map(MAPPING_HBASE_QUALIFIER.stringValue -> destination))
        val time = hBaseHandler.get(MAPPING_HBASE_STORAGE.stringValue, source)
          .getColumnLatestCell(HBASE_COLUMN_FAMILY.bytesValue, MAPPING_HBASE_QUALIFIER.bytesValue).getTimestamp
        this.mapping += (source -> (destination, time))
    }

    /**
     * 获取一条映射关系的value
     *
     * @param source key
     * @return
     */
    override def get(source: String): (String, Long) = {
        if (this.mapping.contains(source))
            this.mapping(source)
        else
            (null, System.currentTimeMillis())
    }

    /**
     * 删除一条映射关系
     *
     * @param source key
     */
    override def remove(source: String): Unit = {
        this.mapping.remove(source)
        hBaseHandler.delete(MAPPING_HBASE_STORAGE.stringValue, source)
    }

    /**
     * 刷新映射关系
     */
    def refresh(): Unit = {
        this.mapping.clear()
        hBaseHandler.get(MAPPING_HBASE_STORAGE.stringValue)
          .map(r => {
              val rk = Bytes.toString(r.getRow)
              val cell = r.getColumnLatestCell(HBASE_COLUMN_FAMILY.bytesValue, MAPPING_HBASE_QUALIFIER.bytesValue)
              val value = Bytes.toString(CellUtil.cloneValue(cell))
              val time = cell.getTimestamp
              rk -> (value, time)
          })
          .filter(r => hBaseHandler.tableExists(r._2._1))
          .foreach(mapping += _)
    }

}
