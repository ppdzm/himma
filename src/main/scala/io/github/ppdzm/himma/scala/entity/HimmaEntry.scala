package io.github.ppdzm.himma.scala.entity

import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{Entry, RowChange}
import io.github.ppdzm.utils.universal.base.{DateTimeUtils, SQLAnalyser}
import io.github.ppdzm.utils.universal.cli.PrettyBricks
import io.github.ppdzm.utils.universal.formats.json.Json4sUtils
import io.github.ppdzm.utils.universal.implicits.BasicConversions.StringImplicits

import java.util.{List => JList}
import scala.collection.JavaConversions._

/**
 * Created by Stuart Alex on 2017/6/2.
 */
object HimmaEntry {
    private val format = "yyyy-MM-dd HH:mm:ss.SSS"

    /**
     * 将com.alibaba.otter.canal.protocol.CanalEntry.Entry解析为HimmaEntry
     *
     * @param e com.alibaba.otter.canal.protocol.CanalEntry.Entry
     * @return
     */
    def apply(e: Entry): HimmaEntry = {
        val database = e.getHeader.getSchemaName
        val table = e.getHeader.getTableName
        val executeTime = e.getHeader.getExecuteTime
        val rowChange = RowChange.parseFrom(e.getStoreValue)
        val ddl = if (rowChange.getIsDdl) {
            SQLAnalyser.squeeze(rowChange.getSql)
        }
        else
            null
        val dataList = rowChange.getRowDatasList.map(data => {
            val afterColumns = data.getAfterColumnsList.map(a => HimmaColumn(a.getName, a.getIsKey, a.getUpdated, a.getIsNull, a.getValue)).toList
            val beforeColumns = data.getBeforeColumnsList.map(b => HimmaColumn(b.getName, b.getIsKey, b.getUpdated, b.getIsNull, b.getValue)).toList
            HimmaRowData(beforeColumns, afterColumns)
        }).toList
        HimmaEntry(database, table, executeTime, rowChange.getEventType.name(), ddl, dataList)
    }

    def display(e: Entry): Unit = {
        val rowChange = RowChange.parseFrom(e.getStoreValue)
        val eventType = rowChange.getEventType
        println(s"${DateTimeUtils.nowWithFormat(format)}==========>time[${DateTimeUtils.format(e.getHeader.getExecuteTime, format)}], binlog[${e.getHeader.getLogfileName}:${e.getHeader.getLogfileOffset}], table[${e.getHeader.getSchemaName},${e.getHeader.getTableName}], eventType : ${eventType.name()}")
        rowChange.getRowDatasList.foreach {
            row =>
                eventType match {
                    case CanalEntry.EventType.DELETE => printColumnList(row.getBeforeColumnsList)
                    case CanalEntry.EventType.UPDATE => printColumnList(row.getBeforeColumnsList, row.getAfterColumnsList)
                    case CanalEntry.EventType.INSERT => printColumnList(row.getAfterColumnsList)
                }
        }
    }

    private def printColumnList(columnList: JList[CanalEntry.Column]): Unit = {
        val maxColumnNameLength: Int = columnList.map(_.getName.length).max
        columnList.foreach {
            column => println(s"${column.getName.pad(maxColumnNameLength, ' ', 1)} : ${column.getValue}")
        }
    }

    private def printColumnList(beforeColumnList: JList[CanalEntry.Column], afterColumnList: JList[CanalEntry.Column]): Unit = {
        val maxColumnNameLength: Int = afterColumnList.filter(_.getUpdated).map(_.getName.length).max
        val beforeDic = beforeColumnList.map(c => c.getName -> c.getValue).toMap
        afterColumnList.filter(_.getUpdated).sortBy(_.getName.length).foreach {
            column =>
                val columnName = column.getName
                val afterValue = if (column.getValue.length > 100) column.getValue.take(100) + "..." else column.getValue
                val beforeValue = if (beforeDic(columnName).length > 100) beforeDic(columnName).take(100) + "..." else beforeDic(columnName)
                val maxValueLength = Array(afterValue.length, beforeValue.length, "↓updated to↓".length).max
                val up = s"${" " * maxColumnNameLength} ${PrettyBricks.rowLeftTopAngle}${beforeValue.pad(maxValueLength, ' ', 0)}${PrettyBricks.headerRightTopAngle}"
                val center = s"${columnName.pad(maxColumnNameLength, ' ', 1)} ${PrettyBricks.rowRightT}${"↓updated to↓".pad(maxValueLength, ' ', 0)}${PrettyBricks.rowVertical}"
                val down = s"${" " * maxColumnNameLength} ${PrettyBricks.rowLeftBottomAngle}${afterValue.pad(maxValueLength, ' ', 0)}${PrettyBricks.rowRightBottomAngle}"
                println(up)
                println(center)
                println(down)
        }
    }

}

/**
 * 解析后的com.alibaba.otter.canal.protocol.CanalEntry.Entry
 *
 * @param database    mysql数据库
 * @param table       mysql表
 * @param executeTime 执行时间
 * @param ddl         ddl
 * @param rowDataList 受影响的行
 */
case class HimmaEntry(database: String, table: String, executeTime: Long, eventType: String, ddl: String, rowDataList: List[HimmaRowData]) {

    override def toString: String = Json4sUtils.serialize4s(this)

}

/**
 * 受一次操作影响的单行详情
 *
 * @param beforeColumns 受影响前的列信息
 * @param afterColumns  受影响后的列信息
 */
case class HimmaRowData(beforeColumns: List[HimmaColumn], afterColumns: List[HimmaColumn])

/**
 * mysql列信息
 *
 * @param columnName 名称
 * @param isKey      是否为主键
 * @param updated    数值是否发生更改
 * @param isNull     是否为空
 * @param value      数值
 */
case class HimmaColumn(columnName: String, isKey: Boolean, updated: Boolean, isNull: Boolean, value: String)
