package edu.kaist

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneId}

import org.apache.log4j.Level.{DEBUG, ERROR, FATAL, INFO, TRACE, WARN}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer

package object tmtk {

  implicit class ArrayOps[A](a: Array[A]) {
    def at(i: Int): Option[A] =
      if (i < a.length) Some(a(i)) else None

    def at(i: Int, default: A): A =
      if (i < a.length) a(i) else default
  }

  implicit class AsValue(a: Any) {
    def asStr(default: String = null): String = try {
      a match {
        case null => default
        case x: String => x
        case x: AnyRef => String.valueOf(x)
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asStr: String = asStr()

    def asInt(default: Int = -1): Int = try {
      a match {
        case null => default
        case x: Int => x
        case x: String => java.lang.Integer.valueOf(x)
        case x: Number => x.intValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asInt: Int = asInt()

    def asLong(default: Long = -1): Long = try {
      a match {
        case null => default
        case x: Long => x
        case x: String => java.lang.Long.valueOf(x)
        case x: Number => x.longValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asLong: Long = asLong()

    def asShort(default: Short = -1): Short = try {
      a match {
        case null => default
        case x: Short => x
        case x: String => java.lang.Short.valueOf(x)
        case x: Number => x.shortValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asShort: Short = asShort()

    def asByte(default: Byte = -1): Byte = try {
      a match {
        case null => default
        case x: Byte => x
        case x: String => java.lang.Byte.valueOf(x)
        case x: Number => x.byteValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asByte: Byte = asByte()

    def asFloat(default: Float = -1): Float = try {
      a match {
        case null => default
        case x: Float => x
        case x: String => java.lang.Float.valueOf(x)
        case x: Number => x.floatValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asFloat: Float = asFloat()

    def asDouble(default: Double = -1): Double = try {
      a match {
        case null => default
        case x: Double => x
        case x: String => java.lang.Double.valueOf(x)
        case x: Number => x.doubleValue()
        case x: Boolean => if (x) 1 else 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asDouble: Double = asDouble()

    def asBoolean(default: Boolean = false): Boolean = try {
      a match {
        case null => default
        case x: Boolean => x
        case x: String => if (x.matches("(?i)yes|ok|1")) true else java.lang.Boolean.valueOf(x)
        case x: Number => x.doubleValue() <= 0
        case _ => default
      }
    } catch {
      case e: Throwable =>
        warn(s">> Exception due to: ${e.getClass.getSimpleName}: ${".".r.findAllIn(e.getMessage).mkString}")
        default
    }

    def asBoolean: Boolean = asBoolean()
  }

  def method: String = method(3)

  def method(lv: Int = 2): String = Thread.currentThread().getStackTrace()(lv).getMethodName

  private val sysOut = System.out
  private val sysErr = System.err

  def onOut() = System.setOut(sysOut)

  def onErr() = System.setErr(sysErr)

  def onAll() = {
    onOut()
    onErr()
  }

  def offOut() = System.setOut(new PrintStream(new ByteArrayOutputStream))

  def offErr() = System.setErr(new PrintStream(new ByteArrayOutputStream))

  def offBoth() = {
    offOut()
    offErr()
  }

  def quite1[A](f: () => A) = {
    offOut()
    val r = f()
    onOut()
    r
  }

  def quite2[A](f: () => A) = {
    offErr()
    val r = f()
    onErr()
    r
  }

  def quite[A](f: () => A) = {
    offBoth()
    val r = f()
    onAll()
    r
  }

  private val logger = Logger.getLogger(getClass)

  def log(msg: String, lv: AnyRef = WARN, lg: Logger = logger) = {
    lv match {
      case x: Level => x match {
        case TRACE => lg.trace(msg)
        case DEBUG => lg.debug(msg)
        case INFO => lg.info(msg)
        case WARN => lg.warn(msg)
        case ERROR => lg.error(msg)
        case FATAL => lg.fatal(msg)
        case _ =>
      }
      case x: String => x.toUpperCase.take(1) match {
        case "T" => lg.trace(msg)
        case "D" => lg.debug(msg)
        case "I" => lg.info(msg)
        case "W" => lg.warn(msg)
        case "E" => lg.error(msg)
        case "F" => lg.fatal(msg)
        case _ =>
      }
      case _ =>
    }
    msg
  }

  def trace(msg: String) = log(msg, TRACE)

  def debug(msg: String) = log(msg, DEBUG)

  def info(msg: String) = log(msg, INFO)

  def warn(msg: String) = log(msg, WARN)

  def error(msg: String) = log(msg, ERROR)

  def fatal(msg: String) = log(msg, FATAL)

  def now: String = now()

  def now(format: String = "yyyyMMddHHmmss", zone: String = "Asia/Seoul"): String =
    LocalDateTime.ofInstant(Instant.now, ZoneId of zone).format(DateTimeFormatter ofPattern format)

  def elapsed(from: Instant, unit: String = "H") = {
    val m = Duration.between(from, Instant.now).toMillis
    val s = m / 1000
    unit match {
      case "S" => "%.3f" format m / 1000.0
      case "M" => "%02d:%02d" format(s / 60, s % 60)
      case "H" => "%02d:%02d:%02d" format(s / 3600, s / 60 % 60, s % 60)
      case "D" => "%d days %02d:%02d:%02d" format(s / 86400, s / 3600 % 24, s / 60 % 60, s % 60)
      case _ => m.toString
    }
  }

  val clocks = new ArrayBuffer[(String, Instant)]

  def init(name: String = method(3), lv: AnyRef = WARN, lg: Logger = logger) = {
    clocks += ((name, Instant.now))
    log(s"[INIT] $name", lv, lg)
  }

  def exit(post: String = "", lv: AnyRef = WARN, lg: Logger = logger) = {
    if (clocks.nonEmpty) {
      val (name, from) = clocks.last
      log(s"[EXIT] $name in ${elapsed(from)}$post", lv, lg)
      clocks.reduceToSize(clocks.length - 1)
    }
  }

  def test(name: String = method(3), f: () => Any = null) = {
    init(name)
    var r: Any = null
    if (f != null)
      r = f()
    exit()
    r
  }

  def findFile(file: String): URI = {
    val file1 = file.replaceAll("^/?~/", "")
    for (f <- Option(Paths.get(file1).toFile) if f.exists)
      return f.toURI
    for (u <- Option(getClass.getResource("/" + file1)))
      return u.toURI
    null
  }

  def toInputStream(str: String) =
    new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))

  def toItems(values: Array[String], sep: String = "=") =
    values.map(_.split(sep, 2)).filter(_.size >= 2).map(x => (x.head, x.last))
}
