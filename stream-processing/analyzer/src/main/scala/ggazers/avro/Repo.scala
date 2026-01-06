/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class Repo(var name_with_owner: Option[String], var owner: Option[String], var name: Option[String], var description: Option[String], var created_at: Option[String], var disk_usage: Option[Long], var visibility: Option[String], var stargazers_count: Option[Long], var forks_count: Option[Long], var watchers_count: Option[Long], var issues_count: Option[Long], var primary_language: Option[String]) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(None, None, None, None, None, None, None, None, None, None, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        name_with_owner match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 1 => {
        owner match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        name match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 3 => {
        description match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 4 => {
        created_at match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 5 => {
        disk_usage match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 6 => {
        visibility match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 7 => {
        stargazers_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 8 => {
        forks_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 9 => {
        watchers_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 10 => {
        issues_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 11 => {
        primary_language match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.name_with_owner = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 1 => this.owner = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 2 => this.name = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 3 => this.description = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 4 => this.created_at = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 5 => this.disk_usage = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 6 => this.visibility = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 7 => this.stargazers_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 8 => this.forks_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 9 => this.watchers_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 10 => this.issues_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 11 => this.primary_language = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.Repo.SCHEMA$
}

object Repo {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Repo\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"name_with_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"disk_usage\",\"type\":[\"long\",\"null\"]},{\"name\":\"visibility\",\"type\":[\"string\",\"null\"]},{\"name\":\"stargazers_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"forks_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"watchers_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"issues_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"primary_language\",\"type\":[\"string\",\"null\"]}]}")
}