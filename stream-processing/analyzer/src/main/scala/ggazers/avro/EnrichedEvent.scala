/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class EnrichedEvent(var event: Option[ggazers.avro.message.PushEvent] = None, var repo: Option[ggazers.avro.message.Repo] = None, var actor: Option[ggazers.avro.message.Actor] = None) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(None, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        event match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 1 => {
        repo match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        actor match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.event = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[ggazers.avro.message.PushEvent]]
      case 1 => this.repo = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[ggazers.avro.message.Repo]]
      case 2 => this.actor = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[ggazers.avro.message.Actor]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.EnrichedEvent.SCHEMA$
}

object EnrichedEvent {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"EnrichedEvent\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"event\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PushEvent\",\"fields\":[{\"name\":\"event\",\"type\":{\"type\":\"record\",\"name\":\"GitHubEvent\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"actor_login\",\"type\":\"string\"},{\"name\":\"repo_name\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"created_at\",\"type\":\"string\"}]}},{\"name\":\"ref\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"repo\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Repo\",\"fields\":[{\"name\":\"name_with_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"disk_usage\",\"type\":[\"long\",\"null\"]},{\"name\":\"visibility\",\"type\":[\"string\",\"null\"]},{\"name\":\"stargazers_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"forks_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"watchers_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"issues_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"primary_language\",\"type\":[\"string\",\"null\"]}]}],\"default\":null},{\"name\":\"actor\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Actor\",\"fields\":[{\"name\":\"login\",\"type\":\"string\"},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"email\",\"type\":[\"string\",\"null\"]},{\"name\":\"bio\",\"type\":[\"string\",\"null\"]},{\"name\":\"company\",\"type\":[\"string\",\"null\"]},{\"name\":\"location\",\"type\":[\"string\",\"null\"]},{\"name\":\"websiteUrl\",\"type\":[\"string\",\"null\"]},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}]}")
}