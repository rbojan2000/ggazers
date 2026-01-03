package ggazers.topology

import ggazers.config.Configuration
import ggazers.serdes.Serdes
import ggazers.avro.message.{GitHubEvent, PushEvent, Repo, Actor, EnrichedEvent, RepoKpi}
import ggazers.joiner.{PushEventActorJoiner, PushEventRepoJoiner}
import ggazers.aggregate.RepoKpiAggregator
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.kstream.{GlobalKTable, Named, SlidingWindows, Windowed}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset

import java.time.Duration

case class TopologyBuilder()
    extends Serdes
    with PushEventActorJoiner
    with PushEventRepoJoiner
    with RepoKpiAggregator
    with LazyLogging {

  val builder: StreamsBuilder = new StreamsBuilder()
  val formatter               = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)

  def build: Topology = {
    val actorsKTable: KTable[String, Actor] =
      builder.table[String, Actor](Configuration.actorsTopic)

    val reposKTable: KTable[String, Repo] = builder.table[String, Repo](Configuration.reposTopic)

    val pushEventsStream: KStream[String, PushEvent] =
      builder.stream[String, PushEvent](Configuration.pushEventsTopic)

    val pushEventsWithActorKey: KStream[String, PushEvent] = pushEventsStream
      .filter((_, pushEvent) => pushEvent.event.actor_login != null)
      .selectKey((_, pushEvent) => pushEvent.event.actor_login)
      .peek((k, v) =>
        logger.whenDebugEnabled {
          logger.debug(s"Processing PushEvent {key: $k} value: $v")
        }
      )

    val eventsEnrichedWithActor: KStream[String, EnrichedEvent] = pushEventsWithActorKey
      .leftJoin(actorsKTable)(joinPushEventWithActor)
      .peek((k, v) =>
        logger.whenDebugEnabled {
          logger.debug(s"Enriched PushEvent with Actor {key: $k} value: $v")
        }
      )

    val eventsWithRepoKey: KStream[String, EnrichedEvent] = eventsEnrichedWithActor
      .filter((_, e) => e.event.exists(_.event.repo_name != null))
      .selectKey((_, e) => e.event.get.event.repo_name)

    val fullyEnrichedEvents: KStream[String, EnrichedEvent] = eventsWithRepoKey
      .leftJoin(reposKTable)(joinPushEventWithRepo)
      .peek((k, v) =>
        logger.whenDebugEnabled {
          logger.debug(s"Enriched PushEvent with Repo {key: $k} value: $v")
        }
      )

    val fullyEnrichedEventsGroupedByRepoStream: KGroupedStream[String, EnrichedEvent] =
      fullyEnrichedEvents
        .filter((_, e) => e.repo.isDefined)
        .groupBy((_, value) => value.event.get.event.repo_name)(
          Grouped.`with`(stringSerde, enrichedEventSerde)
        )

    val repoKpi: KTable[Windowed[String], RepoKpi] = fullyEnrichedEventsGroupedByRepoStream
      .windowedBy(
        SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(Configuration.windowDuration))
      )
      .aggregate(
        initializer = RepoKpi(
          name_with_owner = Some(""),
          name = Some(""),
          owner = Some(""),
          commits_count = Some(0L),
          contributors_count = Some(0L),
          committers_map = Some(Map.empty),
          ggazer_score = Some(0.0),
          best_contributor_login = Some("")
        )
      )(
        aggregate
      )(Materialized.`with`(stringSerde, repoKpiSerde))

    repoKpi
      .toStream(Named.as("processed-repo-kpi"))
      .map { (window, value) =>
        val windowStartMillis = window.window().startTime().toEpochMilli
        val windowEndMillis   = window.window().endTime().toEpochMilli
        val windowStartUtc    = Some(formatter.format(Instant.ofEpochMilli(windowStartMillis)))
        val windowEndUtc      = Some(formatter.format(Instant.ofEpochMilli(windowEndMillis)))
        val withWindow        = value.copy(
          window_start_utc = windowStartUtc,
          window_end_utc = windowEndUtc
        )
        (value.name_with_owner.getOrElse(""), withWindow)
      }
      .selectKey((window, value) => value.name_with_owner.getOrElse(""))
      .peek((k, v) =>
        logger.whenDebugEnabled {
          logger.debug(s"Repo kpi {window: $k} metric: $v")
        }
      )
      .to(Configuration.repoKpiTopic)

    val fullyEnrichedEventsGroupedByActorStream: KGroupedStream[String, EnrichedEvent] =
      fullyEnrichedEvents
        .filter((_, e) => e.actor.isDefined)
        .groupBy((_, value) => value.event.get.event.actor_login)(
          Grouped.`with`(stringSerde, enrichedEventSerde)
        )

    builder.build()
  }
}
