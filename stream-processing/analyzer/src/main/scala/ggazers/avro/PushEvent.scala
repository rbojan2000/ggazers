/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class PushEvent(var event: ggazers.avro.message.GitHubEvent, var ref: Option[String] = None) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(new GitHubEvent, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        event
      }.asInstanceOf[AnyRef]
      case 1 => {
        ref match {
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
      case 1 => this.ref = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.PushEvent.SCHEMA$
}

object PushEvent {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"PushEvent\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"event\",\"type\":{\"type\":\"record\",\"name\":\"GitHubEvent\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"actor_login\",\"type\":\"string\"},{\"name\":\"repo_name\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"created_at\",\"type\":\"string\"}]}},{\"name\":\"ref\",\"type\":[\"null\",\"string\"],\"default\":null}]}")
}