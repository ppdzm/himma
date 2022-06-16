package io.github.ppdzm.himma.scala

import io.github.ppdzm.utils.spark.hive.udf.SubstringIndex
import io.github.ppdzm.utils.universal.base.Symbols._
import org.apache.spark.sql.types.DataTypes
import org.scalatest.FunSuite
import io.github.ppdzm.utils.spark.sql.SparkSQL

import scala.util.Try

/**
 * Created by Stuart Alex on 2017/1/3.
 */
class SimpleTest extends FunSuite {

    test("regex") {
        val regex = "add primary key \\((?<key>.+?)\\)".r
        val line = "add primary key (id)"
        line match {
            case regex(key) => println(key)
            case _ =>
        }
    }

    test("rename table") {
        val renameTableRegex = "alter table (?<before>.+?) rename (?<after>.+)".r
        val statement = s"   ALTER TABLE\t    before_rename     \nRENAME   \nafter_rename    "
        val ddl = {
            var temp = statement.toLowerCase.replace(carriageReturn, " ").replace(lineSeparator, " ").replace("\t", " ").replace(backQuote, " ")
            while (temp.contains("  "))
                temp = temp.replace("  ", " ")
            temp.trim
        }
        println(ddl)
        ddl match {
            case renameTableRegex(before, after) =>
                println("<" + before + ">====<" + after + ">")
                println("<" + before.trim + ">====<" + after.trim + ">")
            case _ =>
        }
    }

    test("data types") {
        val url = "jdbc:mysql://192.168.130.227:3306/la_mordue?user=root&password=123456"
        val data = SparkSQL.mysql.df(url, "datatypes")
        data.show()
        data.schema.map(_.dataType.typeName).foreach(println)
    }

    test("tiny-int") {
        val url = "jdbc:mysql://192.168.130.227:3306/la_mordue?user=root&password=123456&tinyInt1isBit=false"
        val data = SparkSQL.mysql.df(url, "tb")
        data.schema.foreach(sf => println(sf.name + "=>" + sf.dataType.typeName))
        data.show()
        data.withColumn("bl", data("bl").cast(DataTypes.IntegerType)).show()
    }

    test("alter-event-regex") {
        val alterColumnRegex = "alter table (?<table>.+?) (?<changes>.+)".r
        val statement = s"ALTER TABLE `table_name`\nDROP COLUMN `f`,\nMODIFY COLUMN `e`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL AFTER `d`,\nADD COLUMN `f`  varchar(255) NULL DEFAULT '1' AFTER `c`,\nDROP PRIMARY KEY,\nADD PRIMARY KEY (`k`,`e`,`y`),\nCHANGE COLUMN `h` `g`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `e`"
        val ddl = {
            var temp = statement.toLowerCase.replace(carriageReturn, " ").replace(lineSeparator, " ").replace("\t", " ").replace(backQuote, "")
            while (temp.contains("  "))
                temp = temp.replace("  ", " ")
            temp.trim
        }
        ddl match {
            case alterColumnRegex(table, c) => println(s"alter table $table:")
                val addPKRegex = "(?<irrelevant>.+?)primary key \\((?<key>.+?)\\)(?<other>.+?)".r
                val changes = c match {
                    case addPKRegex(irrelevant, key, other) =>
                        println("keys is " + key)
                        c.replace(key, "PRIMARY_KEYS").split(",").map(_.replace("PRIMARY_KEYS", key)).map(_.trim)
                    case _ => c.split(",").map(_.trim)
                }
                changes.foreach(println)
                //add column column  varchar(255) null default '123' after other
                val addColumnRegex = "add column (?<column>.+?)(?<description>.+)".r
                //add primary key (k,e,y)
                val addPrimaryKeyRegex = "add primary key \\((?<key>.+?)\\)".r
                //change column h g  varchar(255) character set utf8 collate utf8_general_ci null default null after e
                val changeColumnRegex = "change column (?<before>.+?) (?<after>.+?)(?<description>.+)".r
                //drop column column
                val dropColumnRegex = "drop column (?<column>.+?)".r
                //drop primary key
                val dropPrimaryKeyRegex = "drop primary key".r
                //modify column e  varchar(255) character set utf8 collate utf8_general_ci not null after d
                val modifyColumnRegex = "modify column (?<column>.+?)(?<description>.+)".r
                changes.foreach {
                    case addColumnRegex(column, description) => println(s"add column “$column” to table “$table”, description is “$description”")
                    case addPrimaryKeyRegex(key) => println(s"add primary key “${key.replace("", "")}” to table “$table”")
                    case changeColumnRegex(before, after, description) => println(s"rename column of table “$table” from “$before” to “$after”, description is “$description”")
                    case dropColumnRegex(column) => println(s"drop column “$column” in table “$table”")
                    case dropPrimaryKeyRegex() => println(s"drop primary key of table “$table”")
                    case modifyColumnRegex(column, description) => println(s"modify column “$column” of table “$table”, description is “$description”")
                    case sub => println(sub)
                }
            case _ => println("unmatched")
        }
    }

    test("format") {
        val a = "alpha"
        val b = "beta"
        val c = "charlie"
        val d = "delta"
        val f = "%s is %s of %s"
        val less = Try(f.format(a, b))
        if (less.isSuccess)
            println(less.get)
        else
            less.failed.get.printStackTrace()
        val more = Try(f.format(a, b, c, d))
        if (more.isSuccess)
            println(more.get)
        else
            more.failed.get.printStackTrace()
    }

    test("substring_index") {
        val substringIndex = new SubstringIndex()
        val source = "yixiaxia"
        val separator = "ia"
        println(substringIndex.evaluate(source, separator, 0))
        println(substringIndex.evaluate(source, separator, 1))
        println(substringIndex.evaluate(source, separator, 2))
        println(substringIndex.evaluate(source, separator, 3))
        println(substringIndex.evaluate(source, separator, -1))
        println(substringIndex.evaluate(source, separator, -2))
        println(substringIndex.evaluate(source, separator, -3))
    }

}
