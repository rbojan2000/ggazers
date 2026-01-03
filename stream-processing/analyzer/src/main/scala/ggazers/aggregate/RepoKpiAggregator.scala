package ggazers.aggregate

import ggazers.avro.message.{EnrichedEvent, RepoKpi}

//   def aggregate(key: String, cityAqiInfo: CityAqiInfo, metric: CountryAirQualityMetrics): CountryAirQualityMetrics = {
// EnrichedEvent(var event: Option[ggazers.avro.message.PushEvent] = None, var repo: Option[ggazers.avro.message.Repo] = None, var actor: Option[ggazers.avro.message.Actor] = None) extends org.apache.avro.specific.SpecificRecordBase {
// Repo(var name_with_owner: Option[String], var owner: Option[String], var name: Option[String], var description: Option[String], var created_at: Option[String], var disk_usage: Option[Long], var visibility: Option[String], var stargazers_count: Option[Long], var forks_count: Option[Long], var watchers_count: Option[Long], var issues_count: Option[Long], var primary_language: Option[String]) extends org.apache.avro.specific.SpecificRecordBase {

trait RepoKpiAggregator {
  def aggregate(key: String, enrichedEvent: EnrichedEvent, aggregate: RepoKpi): RepoKpi =
    RepoKpi(
      name_with_owner = enrichedEvent.repo.flatMap(_.name_with_owner),
      name = enrichedEvent.repo.flatMap(_.name),
      owner = enrichedEvent.repo.flatMap(_.owner),
      commits_count = Some(calculateCommitsCount(enrichedEvent, aggregate)),
      committers_map = Some(updateCommittersMap(enrichedEvent, aggregate)),
      best_contributor_login =
        get_best_contributor_login(updateCommittersMap(enrichedEvent, aggregate)),
      contributors_count =
        Some(get_contributors_count(updateCommittersMap(enrichedEvent, aggregate))),
      ggazer_score = Some(
        calculateGgazerScore(
          calculateCommitsCount(enrichedEvent, aggregate),
          get_contributors_count(updateCommittersMap(enrichedEvent, aggregate))
        )
      )
    )

  protected def get_best_contributor_login(committers_map: Map[String, Long]): Option[String] =
    if (committers_map.isEmpty) {
      None
    } else {
      Some(committers_map.maxBy(_._2)._1)
    }

  protected def calculateCommitsCount(enrichedEvent: EnrichedEvent, aggregate: RepoKpi): Long = {
    val existingCount = aggregate.commits_count.getOrElse(0L)
    val newCommits    = 1L
    existingCount + newCommits
  }

  protected def updateCommittersMap(
      enrichedEvent: EnrichedEvent,
      aggregate: RepoKpi
  ): Map[String, Long] = {
    val existingMap = aggregate.committers_map.getOrElse(Map.empty)
    enrichedEvent.event match {
      case Some(event) =>
        val actorLogin   = event.event.actor_login
        val currentCount = existingMap.getOrElse(actorLogin, 0L)
        existingMap + (actorLogin -> (currentCount + 1L))
      case None        => existingMap
    }
  }

  protected def get_contributors_count(committers_map: Map[String, Long]): Long =
    committers_map.size.toLong

  protected def calculateGgazerScore(commitsCount: Long, contributorsCount: Long): Double =
    commitsCount * 0.5 + contributorsCount * 1.5

}
