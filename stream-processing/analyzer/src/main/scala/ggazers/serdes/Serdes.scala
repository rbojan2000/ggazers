package ggazers.serdes

import ggazers.avro.message.{GitHubEvent, PushEvent, Repo, Actor, EnrichedEvent, RepoKpi, ActorKpi}
import ggazers.utils.AvroSupport
import org.apache.kafka.common.serialization.Serde

trait Serdes extends AvroSupport {

  implicit val pushEventSerde: Serde[PushEvent]         = avroSerde()
  implicit val repoSerde: Serde[Repo]                   = avroSerde()
  implicit val actorSerde: Serde[Actor]                 = avroSerde()
  implicit val enrichedEventSerde: Serde[EnrichedEvent] = avroSerde()
  implicit val repoKpiSerde: Serde[RepoKpi]             = avroSerde()
  implicit val actorKpiSerde: Serde[ActorKpi]           = avroSerde()

}
