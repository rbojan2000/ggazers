package ggazers.joiner
import ggazers.avro.message.{Repo, EnrichedEvent}
import com.typesafe.scalalogging.LazyLogging

trait PushEventRepoJoiner extends LazyLogging {
  def joinPushEventWithRepo: (EnrichedEvent, Repo) => EnrichedEvent = {
    (enrichedEvent: EnrichedEvent, repo: Repo) =>
      EnrichedEvent(
        event = enrichedEvent.event,
        actor = enrichedEvent.actor,
        repo = Option(repo)
      )
  }
}
