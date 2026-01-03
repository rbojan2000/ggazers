/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class ReleaseEvent(var event: ggazers.avro.message.GitHubEvent, var action: Option[String], var release_tag: Option[String]) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(new GitHubEvent, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        event
      }.asInstanceOf[AnyRef]
      case 1 => {
        action match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        release_tag match {
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
        value
      }.asInstanceOf[ggazers.avro.message.GitHubEvent]
      case 1 => this.action = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 2 => this.release_tag = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.ReleaseEvent.SCHEMA$
}

object ReleaseEvent {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ReleaseEvent\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"event\",\"type\":{\"type\":\"record\",\"name\":\"GitHubEvent\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"actor_login\",\"type\":\"string\"},{\"name\":\"repo_name\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"created_at\",\"type\":\"string\"}]}},{\"name\":\"action\",\"type\":[\"string\",\"null\"]},{\"name\":\"release_tag\",\"type\":[\"string\",\"null\"]}]}")
}