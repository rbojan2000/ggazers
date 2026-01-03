/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package ggazers.avro.message

import scala.annotation.switch

final case class UserKpi(var login: String, var codingActionsNum: Int, var differentReposNum: Int, var mostActionsRepo: String, var differentOrgs: Int, var ggazer_score: Float, var ggazer_rank: Int) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", 0, 0, "", 0, 0.0F, 0)
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        login
      }.asInstanceOf[AnyRef]
      case 1 => {
        codingActionsNum
      }.asInstanceOf[AnyRef]
      case 2 => {
        differentReposNum
      }.asInstanceOf[AnyRef]
      case 3 => {
        mostActionsRepo
      }.asInstanceOf[AnyRef]
      case 4 => {
        differentOrgs
      }.asInstanceOf[AnyRef]
      case 5 => {
        ggazer_score
      }.asInstanceOf[AnyRef]
      case 6 => {
        ggazer_rank
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.login = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.codingActionsNum = {
        value
      }.asInstanceOf[Int]
      case 2 => this.differentReposNum = {
        value
      }.asInstanceOf[Int]
      case 3 => this.mostActionsRepo = {
        value.toString
      }.asInstanceOf[String]
      case 4 => this.differentOrgs = {
        value
      }.asInstanceOf[Int]
      case 5 => this.ggazer_score = {
        value
      }.asInstanceOf[Float]
      case 6 => this.ggazer_rank = {
        value
      }.asInstanceOf[Int]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ggazers.avro.message.UserKpi.SCHEMA$
}

object UserKpi {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"UserKpi\",\"namespace\":\"ggazers.avro.message\",\"fields\":[{\"name\":\"login\",\"type\":\"string\"},{\"name\":\"codingActionsNum\",\"type\":\"int\"},{\"name\":\"differentReposNum\",\"type\":\"int\"},{\"name\":\"mostActionsRepo\",\"type\":\"string\"},{\"name\":\"differentOrgs\",\"type\":\"int\"},{\"name\":\"ggazer_score\",\"type\":\"float\"},{\"name\":\"ggazer_rank\",\"type\":\"int\"}]}")
}