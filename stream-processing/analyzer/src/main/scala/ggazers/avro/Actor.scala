/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class Actor(var login: String, var name: Option[String], var email: Option[String], var bio: Option[String], var company: Option[String], var location: Option[String], var websiteUrl: Option[String], var `type`: Option[String] = None) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", None, None, None, None, None, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        login
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
        bio match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 4 => {
        company match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 5 => {
        location match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 6 => {
        websiteUrl match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 7 => {
        `type` match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.login = {
        value.toString
      }.asInstanceOf[String]
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
      case 3 => this.bio = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 4 => this.company = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 5 => this.location = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 6 => this.websiteUrl = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 7 => this.`type` = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.Actor.SCHEMA$
}

object Actor {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Actor\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"login\",\"type\":\"string\"},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"email\",\"type\":[\"string\",\"null\"]},{\"name\":\"bio\",\"type\":[\"string\",\"null\"]},{\"name\":\"company\",\"type\":[\"string\",\"null\"]},{\"name\":\"location\",\"type\":[\"string\",\"null\"]},{\"name\":\"websiteUrl\",\"type\":[\"string\",\"null\"]},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null}]}")
}