package ggazers.aggregate

import ggazers.avro.message.{EnrichedEvent, RepoKpi}

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
