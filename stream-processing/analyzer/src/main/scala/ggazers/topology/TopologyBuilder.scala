package ggazers.topology

import ggazers.config.Configuration
import ggazers.serdes.Serdes
import ggazers.avro.message.{GitHubEvent, PushEvent, Repo, Actor}
// import ggazers.joiner.UserPushEventJoiner
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.kstream.{GlobalKTable, Named, SlidingWindows, Windowed}



import java.time.Duration

case class TopologyBuilder() extends Serdes 
    // with UserPushEventJoiner
    with LazyLogging {

  val builder: StreamsBuilder = new StreamsBuilder()

  def build: Topology = {
    val actorsKTable: KTable[String, Actor] = builder.table[String, Actor](Configuration.actorsTopic)
    val reposKTable: KTable[String, Repo] = builder.table[String, Repo](Configuration.reposTopic)
    val pushEventsStream: KStream[String, PushEvent] = builder.stream[String, PushEvent](Configuration.pushEventsTopic)


    val reposKpiStream: KStream[String, (Repo, Long)] = reposKTable
      .leftJoin(pushEventsStream)(
        (key, pushEvent) => pushEvent.event.repo_name,
        joinPushEventWithRepo
      )


    // val pushEventsWithActorKey: KStream[String, PushEvent] = pushEventsStream
    //   .selectKey((_, pushEvent) => pushEvent.actor_login)
    //   .peek((k, v) =>
    //     logger.info(s"Processing PushEvent {key: $k} value: $v")
    //   )

    // val citiesGlobalTable: GlobalKTable[String, City] = builder.globalTable[String, City](Configuration.citiesTopic)

    // val cityWithAirPollutantLevelStream: KStream[String, AirQualityWithPollutionLevel] = airQualityStream
    //   .filter((_, v) => v.aqi.isDefined)
    //   .mapValues(
    //     mapToCityAirPollutantLevel
    //   )

    // cityWithAirPollutantLevelStream
    //   .to(Configuration.cityairpollutantTopic)

    // val airQualityWithCityStream: KStream[String, CityAqiInfo] = cityWithAirPollutantLevelStream
    //   .leftJoin(citiesGlobalTable)(
    //     (key, _) => key,
    //     joinAqiWithCity
    //   )
    //   .peek((k, v) =>
    //     logger.whenDebugEnabled {
    //       logger.debug(s"Enriched air quality {city: $k} with: $v")
    //     }
    //   )

    // val airQualityWithCityGroupedStream: KGroupedStream[String, CityAqiInfo] =
    //   airQualityWithCityStream
    //     .groupBy((_, value) => value.country)(Grouped.`with`(stringSerde, cityAqiInfoSerde))

    // val windowedCountryAirQualityMetrics: KTable[Windowed[String], CountryAirQualityMetrics] = airQualityWithCityGroupedStream
    //   .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(Configuration.windowDuration)))
    //   .aggregate(
    //     initializer = CountryAirQualityMetrics("", CityMetric("", 1, "", ""), CityMetric("", 1, "", ""), CityMetric("", 1, "", ""), "", Map.empty, 0, 0)
    //   )(
    //     aggregate
    //   )(Materialized.`with`(stringSerde, countryAirQualityMetricsSerde))


    // windowedCountryAirQualityMetrics
    //   .toStream(Named.as("processed-country-air-quality-metrics"))
    //   .selectKey((window, value) => value.country)
    //   .peek((k, v) =>
    //     logger.whenDebugEnabled {
    //       logger.debug(s"Country metrics {window: $k} metric: $v")
    //     })
    //   .to(Configuration.countryAqiMetricsTopic)

    builder.build()
  }
}