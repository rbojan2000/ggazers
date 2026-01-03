package ggazers.config

import com.typesafe.config.ConfigFactory

object Configuration {

  private val config = ConfigFactory.load("application.conf")

  val schemaRegistryUrl: String = config.getString("kafka.schema-registry-url")
  val bootstrapServers: String  = config.getString("kafka.bootstrap-servers")

  val reposTopic: String      = config.getString("kafka.topics.repos")
  val actorsTopic: String     = config.getString("kafka.topics.actors")
  val pushEventsTopic: String = config.getString("kafka.topics.push-events")
  val repoKpiTopic: String    = config.getString("kafka.topics.repo-kpi")

  val windowDuration: Int = config.getInt("kafka.sliding-windows-duration-mins")

  val appId: String = config.getString("ggazers.app-id")

}
