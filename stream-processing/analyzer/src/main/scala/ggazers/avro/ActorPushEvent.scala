/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class ActorPushEvent(var login: String, var repo_full_name: String, var repo_name: String, var repo_owner: String, var repo_owner_actor_type: String) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", "", "", "", "")
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        login
      }.asInstanceOf[AnyRef]
      case 1 => {
        repo_full_name
      }.asInstanceOf[AnyRef]
      case 2 => {
        repo_name
      }.asInstanceOf[AnyRef]
      case 3 => {
        repo_owner
      }.asInstanceOf[AnyRef]
      case 4 => {
        repo_owner_actor_type
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.login = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.repo_full_name = {
        value.toString
      }.asInstanceOf[String]
      case 2 => this.repo_name = {
        value.toString
      }.asInstanceOf[String]
      case 3 => this.repo_owner = {
        value.toString
      }.asInstanceOf[String]
      case 4 => this.repo_owner_actor_type = {
        value.toString
      }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.ActorPushEvent.SCHEMA$
}

object ActorPushEvent {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ActorPushEvent\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"login\",\"type\":\"string\"},{\"name\":\"repo_full_name\",\"type\":\"string\"},{\"name\":\"repo_name\",\"type\":\"string\"},{\"name\":\"repo_owner\",\"type\":\"string\"},{\"name\":\"repo_owner_actor_type\",\"type\":\"string\"}]}")
}