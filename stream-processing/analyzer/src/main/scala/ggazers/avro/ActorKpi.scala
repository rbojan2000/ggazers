/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class ActorKpi(var actor_login: Option[String], var name: Option[String], var email: Option[String], var commits_count: Option[Long], var repos_contributed_to_count: Option[Long], var most_contributed_repo_name: Option[String], var repos_map: Option[Map[String, Long]] = None, var ggazer_score: Option[Double] = None, var window_start_utc: Option[String] = None, var window_end_utc: Option[String] = None) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(None, None, None, None, None, None, None, None, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        actor_login match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 1 => {
        name match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        email match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 3 => {
        commits_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 4 => {
        repos_contributed_to_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 5 => {
        most_contributed_repo_name match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 6 => {
        repos_map match {
          case Some(x) => {
            val map: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any]
            x foreach { kvp =>
              val key = kvp._1
              val value = kvp._2
              map.put(key, value)
            }
            map
          }
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 7 => {
        ggazer_score match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 8 => {
        window_start_utc match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 9 => {
        window_end_utc match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.actor_login = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 1 => this.name = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 2 => this.email = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 3 => this.commits_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 4 => this.repos_contributed_to_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 5 => this.most_contributed_repo_name = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 6 => this.repos_map = {
        value match {
          case null => None
          case _ => Some(value match {
            case (map: java.util.Map[_,_]) => {
              scala.jdk.CollectionConverters.MapHasAsScala(map).asScala.toMap map { kvp =>
                val key = kvp._1.toString
                val value = kvp._2
                (key, value)
              }
            }
          })
        }
      }.asInstanceOf[Option[Map[String, Long]]]
      case 7 => this.ggazer_score = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Double]]
      case 8 => this.window_start_utc = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 9 => this.window_end_utc = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.ActorKpi.SCHEMA$
}

object ActorKpi {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ActorKpi\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"actor_login\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"email\",\"type\":[\"string\",\"null\"]},{\"name\":\"commits_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"repos_contributed_to_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"most_contributed_repo_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"repos_map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\"}],\"default\":null},{\"name\":\"ggazer_score\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"window_start_utc\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"window_end_utc\",\"type\":[\"null\",\"string\"],\"default\":null}]}")
}