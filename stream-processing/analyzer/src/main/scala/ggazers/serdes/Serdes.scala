package ggazers.serdes

import ggazers.avro.message.{GitHubEvent, PushEvent, Repo, Actor}
import ggazers.utils.AvroSupport
import org.apache.kafka.common.serialization.Serde

trait Serdes extends AvroSupport {

  implicit val pushEventSerde: Serde[PushEvent] = avroSerde()
  implicit val repoSerde: Serde[Repo] = avroSerde()
  implicit val actorSerde: Serde[Actor] = avroSerde()
}
