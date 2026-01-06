package ggazers.aggregate

import ggazers.avro.message.{EnrichedEvent, ActorKpi}

trait ActorKpiAggregator {
  def aggregate(key: String, enrichedEvent: EnrichedEvent, aggregate: ActorKpi): ActorKpi =
    ActorKpi(
      actor_login = enrichedEvent.actor.map(_.login),
      name = enrichedEvent.actor.flatMap(_.name),
      email = enrichedEvent.actor.flatMap(_.email),
      commits_count = Some(calculateCommitsCount(enrichedEvent, aggregate)),
      repos_map = Some(updateReposMap(enrichedEvent, aggregate)),
      repos_contributed_to_count =
        Some(getReposContributedToCount(updateReposMap(enrichedEvent, aggregate))),
      most_contributed_repo_name =
        getMostContributedRepoName(updateReposMap(enrichedEvent, aggregate))
    )

  protected def calculateCommitsCount(enrichedEvent: EnrichedEvent, aggregate: ActorKpi): Long = {
    val existingCount = aggregate.commits_count.getOrElse(0L)
    val newCommits    = 1L
    existingCount + newCommits
  }

  protected def updateReposMap(
      enrichedEvent: EnrichedEvent,
      aggregate: ActorKpi
  ): Map[String, Long] = {
    val existingMap = aggregate.repos_map.getOrElse(Map.empty)
    enrichedEvent.event match {
      case Some(event) =>
        val repoName     = event.event.repo_name
        val currentCount = existingMap.getOrElse(repoName, 0L)
        existingMap + (repoName -> (currentCount + 1L))
      case None        => existingMap
    }
  }

  protected def getReposContributedToCount(reposMap: Map[String, Long]): Long =
    reposMap.size.toLong

  protected def getMostContributedRepoName(reposMap: Map[String, Long]): Option[String] =
    if (reposMap.isEmpty) {
      None
    } else {
      Some(reposMap.maxBy(_._2)._1)
    }
}
