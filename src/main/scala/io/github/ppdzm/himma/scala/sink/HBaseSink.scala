package io.github.ppdzm.himma.scala.sink

import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.entity.HimmaEntry

import java.text.SimpleDateFormat
import java.util.Date

case class HBaseSink(himma: Himma) extends Sink {
    private lazy val siDateFormatter = new SimpleDateFormat("yyyyMMdd")

    import himma._
    import himma.himmaConfig._

    override def sink(e: HimmaEntry): Unit = {
        if (!HBASE_SINK_ENABLED.booleanValue)
            return
        val mappingKey = himma.generateMappingKey(e.database, e.table)
        val hTableName = himma.mappingRecorder.get(mappingKey)._1
        val incremental = HBASE_INCREMENT_TABLES.arrayValue(CONFIG_SEPARATOR.stringValue).contains(hTableName)
        val incrementalHTable = hTableName + "-" + this.siDateFormatter.format(new Date(e.executeTime))
        EventType.valueOf(e.eventType) match {
            case EventType.ALTER =>
                if (hTableName != null) {
                    ddlSynchronizer.synchronizeAlter(e, hTableName)
                }
            case EventType.CINDEX =>
            case EventType.CREATE => ddlSynchronizer.synchronizeCreate()
            case EventType.DELETE =>
                if (hTableName != null) {
                    deleteSynchronizer.synchronizeDelete(e, hTableName, HBASE_COLUMN_FAMILY.stringValue)
                    if (incremental)
                        deleteSynchronizer.markAsDelete(e, incrementalHTable, HBASE_COLUMN_FAMILY.stringValue)
                }
            case EventType.DINDEX =>
            case EventType.ERASE =>
                if (hTableName != null) {
                    ddlSynchronizer.synchronizeDrop(e, hTableName, HBASE_COLUMN_FAMILY.stringValue)
                }
            case EventType.INSERT =>
                if (hTableName != null) {
                    insertSynchronizer.synchronizeInsert(e, hTableName, HBASE_COLUMN_FAMILY.stringValue)
                    if (incremental)
                        insertSynchronizer.markAsInsert(e, incrementalHTable, HBASE_COLUMN_FAMILY.stringValue)
                }
            case EventType.QUERY =>
            case EventType.RENAME =>
                val ddl = e.ddl
                val renameTableRegex = """rename table (?<before>.+?) to (?<after>.+)""".r
                val (realHTable, coequalEntry) = ddl match {
                    case renameTableRegex(before, _) =>
                        val oldKey = himma.generateMappingKey(e.database, before)
                        (mappingRecorder.get(oldKey)._1, HimmaEntry(e.database, before, e.executeTime, e.eventType, e.ddl, e.rowDataList))
                    case _ => (hTableName, e)
                }
                if (realHTable != null) {
                    ddlSynchronizer.synchronizeAlter(coequalEntry, realHTable)
                }
            case EventType.TRUNCATE =>
                if (hTableName != null) {
                    ddlSynchronizer.synchronizeTruncate(e, hTableName, HBASE_COLUMN_FAMILY.stringValue)
                }
            case EventType.UPDATE =>
                if (hTableName != null) {
                    updateSynchronizer.synchronizeUpdate(e, hTableName, HBASE_COLUMN_FAMILY.stringValue)
                    if (incremental)
                        updateSynchronizer.markAsUpdate(e, incrementalHTable, HBASE_COLUMN_FAMILY.stringValue)
                }
            case _ =>
        }
    }

}
