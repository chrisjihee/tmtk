package edu.kaist.tmtk.db

import java.io.Closeable

import com.datastax.driver.core.{Cluster, SimpleStatement, SocketOptions}
import edu.kaist.tmtk._
import resource.managed

import scala.collection.JavaConversions._

class Cassandra(path: String, var table: String = null, var schema: String = null, var order: String = null, reset: Boolean = false, lv: AnyRef = "I") extends Closeable {
  private val Array(host, name) = path.split("/", 2)
  val connection = Cluster.builder.addContactPoint(host).withSocketOptions(new SocketOptions().setConnectTimeoutMillis(180000).setReadTimeoutMillis(600000)).build
  val session = connection.connect()
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $name WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
  session.execute(s"USE $name")
  if (table != null && schema != null)
    create(schema, order)
  log(s"[DONE] Connect $info", lv)
  val info = s"Cassandra($host/$name/${table.asStr("")})"

  override def toString = s"${table.asStr("")}"

  override def close() = connection.close()

  def manage() = managed(this)

  def create(schema: String, order: String = null, table: String = null) = {
    if (table != null)
      this.table = table
    this.schema = schema
    this.order = order
    if (reset)
      session.execute(s"DROP TABLE IF EXISTS ${this.table}")
    if (order == null)
      session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema)")
    else
      session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema) WITH CLUSTERING ORDER BY ($order)")
  }

  def insert(row: AMap[String, Any], table: String = null): Unit = {
    if (table != null)
      this.table = table
    val query = s"INSERT INTO ${this.table}(${row.keys.mkString(", ")}) VALUES (${Array.fill(row.size)("?").mkString(", ")})"
    try {
      session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
    } catch {
      case e: Exception => try {
        Thread.sleep(3000); session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
      } catch {
        case e: Exception => try {
          Thread.sleep(7000); session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
        } catch {
          case e: Exception => try {
            Thread.sleep(10000); session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
          } catch {
            case e: Exception => try {
              Thread.sleep(30000); session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
            } catch {
              case e: Exception => try {
                Thread.sleep(300000); session.execute(new SimpleStatement(query, row.values.map(_.asJObj).toArray: _*))
              } catch {
                case e: Exception => fatal(s"Exception raised: $e"); throw e
              }
            }
          }
        }
      }
    }
  }

  def update(query: String, args: Any*) = try {
    session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
  } catch {
    case e: Exception => try {
      Thread.sleep(3000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
    } catch {
      case e: Exception => try {
        Thread.sleep(7000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
      } catch {
        case e: Exception => try {
          Thread.sleep(10000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
        } catch {
          case e: Exception => try {
            Thread.sleep(30000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
          } catch {
            case e: Exception => try {
              Thread.sleep(300000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def size = count(this.table)

  def count(query: String, args: Any*): Long = try {
    session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
  } catch {
    case e: Exception => try {
      Thread.sleep(3000); session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
    } catch {
      case e: Exception => try {
        Thread.sleep(7000); session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
      } catch {
        case e: Exception => try {
          Thread.sleep(10000); session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
        } catch {
          case e: Exception => try {
            Thread.sleep(30000); session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
          } catch {
            case e: Exception => try {
              Thread.sleep(300000); session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def count2(where: String, args: Any*) = count(s"$table WHERE $where", args: _*)

  def one(query: String, args: Any*) = try {
    Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
  } catch {
    case e: Exception => try {
      Thread.sleep(3000); Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
    } catch {
      case e: Exception => try {
        Thread.sleep(7000); Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
      } catch {
        case e: Exception => try {
          Thread.sleep(10000); Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
        } catch {
          case e: Exception => try {
            Thread.sleep(30000); Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
          } catch {
            case e: Exception => try {
              Thread.sleep(300000); Option(session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one).map(_.getObject(0)).orNull
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def one2(select: String) = one(s"SELECT $select FROM $table")

  def one2(select: String, where: String, args: Any*) = one(s"SELECT $select FROM $table WHERE $where", args: _*)

  def ones(query: String, args: Any*) = try {
    session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
  } catch {
    case e: Exception => try {
      Thread.sleep(3000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
    } catch {
      case e: Exception => try {
        Thread.sleep(7000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
      } catch {
        case e: Exception => try {
          Thread.sleep(10000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
        } catch {
          case e: Exception => try {
            Thread.sleep(30000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
          } catch {
            case e: Exception => try {
              Thread.sleep(300000); session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0))
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def ones2(select: String) = ones(s"SELECT $select FROM $table")

  def ones2(select: String, where: String, args: Any*) = ones(s"SELECT $select FROM $table WHERE $where", args: _*)

  def row(query: String, args: Any*) = try {
    val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
    Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
  } catch {
    case e: Exception => try {
      Thread.sleep(3000)
      val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
      Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
    } catch {
      case e: Exception => try {
        Thread.sleep(7000)
        val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
        Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
      } catch {
        case e: Exception => try {
          Thread.sleep(10000)
          val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
          Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
        } catch {
          case e: Exception => try {
            Thread.sleep(30000)
            val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
            Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
          } catch {
            case e: Exception => try {
              Thread.sleep(300000)
              val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
              Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def row2(select: String) = row(s"SELECT $select FROM $table")

  def row2(select: String, where: String, args: Any*) = row(s"SELECT $select FROM $table WHERE $where", args: _*)

  def rows(query: String, args: Any*) = try {
    val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
    for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
  } catch {
    case e: Exception => try {
      Thread.sleep(3000)
      val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
      for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
    } catch {
      case e: Exception => try {
        Thread.sleep(7000)
        val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
        for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
      } catch {
        case e: Exception => try {
          Thread.sleep(10000)
          val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
          for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
        } catch {
          case e: Exception => try {
            Thread.sleep(30000)
            val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
            for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
          } catch {
            case e: Exception => try {
              Thread.sleep(300000)
              val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
              for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
            } catch {
              case e: Exception => fatal(s"Exception raised: $e"); throw e
            }
          }
        }
      }
    }
  }

  def rows2(select: String) = rows(s"SELECT $select FROM $table")

  def rows2(select: String, where: String, args: Any*) = rows(s"SELECT $select FROM $table WHERE $where", args: _*)

  def map(query: String, args: Any*) = {
    val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
    Option(r).map(r => XMap(r.getColumnDefinitions.map(_.getName).map(k => k -> r.getObject(k)).toSeq: _*)).orNull
  }

  def maps(query: String, args: Any*) = {
    val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
    for (r <- rs.iterator) yield XMap(r.getColumnDefinitions.map(_.getName).map(k => k -> r.getObject(k)).toSeq: _*)
  }
}
