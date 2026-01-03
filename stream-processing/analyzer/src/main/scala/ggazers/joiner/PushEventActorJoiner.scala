package ggazers.joiner

import ggazers.avro.message.{Actor, PushEvent, EnrichedEvent}
import com.typesafe.scalalogging.LazyLogging

trait PushEventActorJoiner extends LazyLogging {
  def joinPushEventWithActor: (PushEvent, Actor) => EnrichedEvent = {
    (pushEvent: PushEvent, actor: Actor) =>
      EnrichedEvent(
        event = Some(pushEvent),
        actor = Option(actor)
      )
  }
}
