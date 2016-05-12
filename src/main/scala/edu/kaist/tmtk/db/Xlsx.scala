package edu.kaist.tmtk.db

import java.io.Closeable
import java.util.Date

import edu.kaist.tmtk._
import org.apache.poi.ss.usermodel.{Row, Sheet, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import resource.managed

import scala.collection.JavaConversions._
import scala.reflect.io.File

class Xlsx(path: String, var table: String = null, var fields: Seq[String] = null, lv: AnyRef = "I") extends Closeable {
  val wb = if (!File(path).exists) new XSSFWorkbook else WorkbookFactory.create(File(path).inputStream())
  var isModified = false
  var sheet: Sheet = null
  if (!File(path).exists && table != null && fields != null) {
    create(fields, table)
  } else if (table != null) {
    this.sheet = wb.getSheet(table)
    this.fields = for (c <- sheet.getRow(0).toList) yield c.toString
  }

  override def toString = s"Xlsx($path/$table)"

  override def close() = {
    if (isModified)
      commit()
    wb.close()
  }

  def commit() = wb.write(File(path).outputStream())

  def manage() = managed(this)

  def create(fields: Seq[String], table: String = null) = {
    if (table != null)
      this.table = table
    this.fields = fields
    this.sheet = wb.createSheet(this.table)
    val r = sheet.createRow(0)
    for ((field, i) <- fields.zipWithIndex) {
      val c = r.createCell(i)
      c.setCellValue(field)
    }
    this.isModified = true
  }

  def insert(row: AMap[String, Any], table: String = null) = {
    if (table != null)
      this.table = table
    val r = sheet.createRow(sheet.getLastRowNum + 1)
    setRow(r, row)
    this.isModified = true
  }

  def delete(i: Int) = {
    if (sheet.getRow(i) != null) {
      sheet.removeRow(sheet.getRow(i))
      this.isModified = true
    }
  }

  def update(i: Int, row: AMap[String, Any]) = {
    val r = sheet.getRow(i)
    setRow(r, row)
    this.isModified = true
  }

  def setRow(r: Row, row: AMap[String, Any]) = {
    for ((field, i) <- fields.zipWithIndex if row.contains(field)) {
      val c = r.createCell(i)
      row(field) match {
        case x: String => c.setCellValue(x)
        case x: Number => c.setCellValue(x.doubleValue)
        case x: Boolean => c.setCellValue(x)
        case x: Date => c.setCellValue(x)
        case _ =>
      }
    }
  }

  def size = sheet.size - 1

  def one(i: Int, j: Int) =
    sheet.getRow(i).getCell(j).asAny

  def one(i: Int, field: String): Any =
    one(i, fields.indexOf(field))

  def ones(j: Int): Stream[Any] =
    for (r <- sheet.toStream.drop(1)) yield r.getCell(j).asAny

  def ones(field: String): Stream[Any] =
    ones(fields.indexOf(field))

  def row(i: Int) =
    for (c <- sheet.getRow(i).toList) yield c.asAny

  def rows =
    for (r <- sheet.toStream.drop(1)) yield for (c <- r.toList) yield c.asAny

  def map(i: Int) = {
    val r = sheet.getRow(i)
    XMap((for ((field, i) <- fields.zipWithIndex) yield field -> r.getCell(i).asAny): _*)
  }

  def maps =
    for (r <- sheet.toStream.drop(1)) yield
      XMap((for ((field, i) <- fields.zipWithIndex) yield field -> r.getCell(i).asAny): _*)
}
