package edu.kaist

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, PrintStream}
import java.lang.Thread.sleep
import java.net.{Socket => JSocket, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.AccessController
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import edu.kaist.tmtk.db.MySQL
import org.apache.log4j.Level.{DEBUG, ERROR, FATAL, INFO, TRACE, WARN}
import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}
import sun.security.action.GetPropertyAction

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.io.Socket

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

  def toJavaList[A](a: A) = seqAsJavaList(Seq(a))

  def method: String = method(3)

  def method(lv: Int = 2): String = Thread.currentThread().getStackTrace()(lv).getMethodName

  def filename: String = filename(3)

  def filename(lv: Int = 2): String = Thread.currentThread().getStackTrace()(lv).getFileName.replaceAll("[.].*$", "")

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

  def quite1[A](f: () => A, q: Boolean = true) = {
    if (q) offOut()
    val r = f()
    if (q) onOut()
    r
  }

  def quite2[A](f: () => A, q: Boolean = true) = {
    if (q) offErr()
    val r = f()
    if (q) onErr()
    r
  }

  def quite[A](f: () => A, q: Boolean = true) = {
    if (q) offBoth()
    val r = f()
    if (q) onAll()
    r
  }

  def pause[A](f: () => A, second: Int = 0) = {
    if (second > 0)
      sleep(second * 1000)
    f()
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

  val clocks = new TrieMap[String, Instant]

  def init(key: String = method(4), lv: AnyRef = WARN, lg: Logger = logger, marker: String = "[INIT]") = {
    clocks += key -> Instant.now
    log(s"$marker $key", lv, lg)
  }

  def exit(key: String = method(4), post: String = "", lv: AnyRef = WARN, lg: Logger = logger, marker: String = "[EXIT]") = {
    if (clocks.contains(key)) {
      val from = clocks(key)
      log(s"$marker $key in ${elapsed(from)}$post", lv, lg)
      clocks -= key
    }
  }

  def test(name: String = method(3), f: () => Any = null, logfile: String = null) = {
    if (logfile != null)
      changeLogfile(logfile)
    init(name)
    var r: Any = null
    if (f != null)
      r = f()
    exit(name)
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

  def changeLogfile(logfile: String, propfile: String = "log4j.properties", key: String = "log4j.appender.FILE.File") = {
    LogManager.resetConfiguration()
    val props = new Properties
    props.load(findFile(propfile).toURL.openStream)
    props.setProperty(key, logfile)
    PropertyConfigurator.configure(props)
  }

  def temp =
    new File(AccessController.doPrivileged(new GetPropertyAction("java.io.tmpdir")))

  def inTemp(child: String) =
    new File(temp, child)

  def toInputStream(str: String) =
    new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))

  def toItems(values: Array[String], sep: String = "=") =
    values.map(_.split(sep, 2)).filter(_.length >= 2).map(x => (x.head, x.last))

  def connect(host: String, port: Int, second: Int = 0) = try {
    pause(() => new Socket(new JSocket(host, port)), second)
  } catch {
    case e: Throwable => null
  }

  def reconnect(host: String, port: Int, seconds: Stream[Int] = 0 #:: 10 #:: 20 #:: Stream(30)) =
    seconds.map(connect(host, port, _)).dropWhile(_ == null).headOption.orNull

  def assign[D](ds: Iterable[D], f: (D, ArrayBuffer[String]) => Any, multi: Int = 1, interval: Int = 1, offset: Long = 0) = {
    val (sleepTimeNext, sleepTimeLast) = (0, 0)
    val (numInit, numDone) = (new AtomicLong(offset), new AtomicLong)
    val jobs = new TrieMap[String, Thread]

    for (d <- ds) {
      if (multi <= 1)
        handle1(d)
      else
        handle2(d)
    }
    while (jobs.nonEmpty)
      Thread.sleep(sleepTimeLast)

    def done = if (numDone.incrementAndGet % interval == 0) "W" else "D"
    def name(n: Long) = "J%010d" format n
    def str(ms: ArrayBuffer[String]) = if (ms.nonEmpty) s" / ${ms.mkString(" / ")}" else ""

    def handle1(d: D, n: String = name(numInit.incrementAndGet)) {
      val ms = new ArrayBuffer[String]
      init(n, "D")
      f(d, ms)
      exit(n, str(ms), done)
    }

    def handle2(d: D, n: String = name(numInit.incrementAndGet)) {
      val ms = new ArrayBuffer[String]
      while (jobs.size >= multi)
        Thread.sleep(sleepTimeNext)
      jobs += n -> new Thread(new Runnable {
        override def run(): Unit = {
          init(n, "D")
          f(d, ms)
          exit(n, str(ms), done)
          jobs -= n
        }
      })
      jobs(n).start()
    }
  }

  def main(args: Array[String]) {
    args.at(0, null) match {
      case "assign" => testAssign()
      case _ =>
    }
  }

  def testAssign() = test(method, () => {
    val data = Stream.iterate(1)(x => x + 1).take(100)
    val out = new MySQL("143.248.48.105/unit", "chrisjihee", "jiheeryu", "assigner", "i int auto_increment, t timestamp default current_timestamp, data int, conf varchar(50), result int, primary key(i)")
    val conf = new ArrayBuffer[String]

    conf += "multi=1"
    test("assign1", () => assign(data, square, multi = 1, interval = 20))
    conf.clear

    conf += "multi=2"
    test("assign2", () => assign(data, square, multi = 2, interval = 20))
    conf.clear

    conf += "multi=5"
    test("assign3", () => assign(data, square, multi = 5, interval = 20))
    conf.clear

    conf += "multi=10"
    test("assign4", () => assign(data, square, multi = 10, interval = 20))
    conf.clear

    def square(d: Int, post: ArrayBuffer[String]) = {
      val result = d * d
      out.insert(Map("data" -> d, "conf" -> conf.mkString(" / "), "result" -> result))
    }
  })
}
