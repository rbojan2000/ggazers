/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class OrganizationKpi(var organization_login: Option[String], var date: Option[String], var commits_count: Option[Long], var repos_contributed_to_count: Option[Long], var contributors_count: Option[Long]) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(None, None, None, None, None)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        organization_login match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 1 => {
        date match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 2 => {
        commits_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 3 => {
        repos_contributed_to_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case 4 => {
        contributors_count match {
          case Some(x) => x
          case None => null
        }
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.organization_login = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 1 => this.date = {
        value match {
          case null => None
          case _ => Some(value.toString)
        }
      }.asInstanceOf[Option[String]]
      case 2 => this.commits_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 3 => this.repos_contributed_to_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case 4 => this.contributors_count = {
        value match {
          case null => None
          case _ => Some(value)
        }
      }.asInstanceOf[Option[Long]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.OrganizationKpi.SCHEMA$
}

object OrganizationKpi {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrganizationKpi\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"organization_login\",\"type\":[\"string\",\"null\"]},{\"name\":\"date\",\"type\":[\"string\",\"null\"]},{\"name\":\"commits_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"repos_contributed_to_count\",\"type\":[\"long\",\"null\"]},{\"name\":\"contributors_count\",\"type\":[\"long\",\"null\"]}]}")
}