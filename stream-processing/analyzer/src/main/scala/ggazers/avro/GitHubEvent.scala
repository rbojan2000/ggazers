/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class GitHubEvent(var id: String, var actor_login: String, var repo_name: String, var event_type: String, var created_at: String) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", "", "", "", "")
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        id
      }.asInstanceOf[AnyRef]
      case 1 => {
        actor_login
      }.asInstanceOf[AnyRef]
      case 2 => {
        repo_name
      }.asInstanceOf[AnyRef]
      case 3 => {
        event_type
      }.asInstanceOf[AnyRef]
      case 4 => {
        created_at
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.id = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.actor_login = {
        value.toString
      }.asInstanceOf[String]
      case 2 => this.repo_name = {
        value.toString
      }.asInstanceOf[String]
      case 3 => this.event_type = {
        value.toString
      }.asInstanceOf[String]
      case 4 => this.created_at = {
        value.toString
      }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.GitHubEvent.SCHEMA$
}

object GitHubEvent {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GitHubEvent\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"actor_login\",\"type\":\"string\"},{\"name\":\"repo_name\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"created_at\",\"type\":\"string\"}]}")
}