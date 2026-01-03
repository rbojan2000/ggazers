package ggazers.aggregate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ggazers.avro.message.{EnrichedEvent, RepoKpi, PushEvent, Repo, Actor, GitHubEvent}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import ggazers.avro.message.{EnrichedEvent, RepoKpi}

class RepoKpiAggregatorTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with RepoKpiAggregator {

  behavior of "RepoKpiAggregator.aggregate"

  it should "increment commits_count and update committers_map and contributors_count" in {
    val initialRepoKpi = RepoKpi(
      name_with_owner = Some("owner/repo"),
      name = Some("repo"),
      owner = Some("owner"),
      commits_count = Some(0L),
      contributors_count = Some(0L),
      best_contributor_login = None,
      committers_map = Some(Map.empty),
      ggazer_score = Some(0.0)
    )

    val pushEvent = PushEvent(
      event = GitHubEvent(
        id = "1",
        actor_login = "alice",
        repo_name = "owner/repo",
        event_type = "PushEvent",
        created_at = "2026-01-06T12:00:00Z"
      ),
      ref = None
    )
    val enrichedEvent = EnrichedEvent(
      event = Some(pushEvent),
      repo = Some(Repo(
        name_with_owner = Some("owner/repo"),
        owner = Some("owner"),
        name = Some("repo"),
        description = None,
        created_at = None,
        disk_usage = None,
        visibility = None,
        stargazers_count = None,
        forks_count = None,
        watchers_count = None,
        issues_count = None,
        primary_language = None
      )),
      actor = Some(Actor(
        login = "alice",
        name = None,
        email = None,
        bio = None,
        company = None,
        location = None,
        websiteUrl = None,
        `type` = None
      ))
    )

    val result = aggregate("owner/repo", enrichedEvent, initialRepoKpi)
    result.commits_count shouldBe Some(1L)
    result.contributors_count shouldBe Some(1L)
    result.committers_map shouldBe Some(Map("alice" -> 1L))
    result.best_contributor_login shouldBe Some("alice")
    result.ggazer_score shouldBe Some(2.0)
  }

  it should "increment commit count for existing committer" in {
    val initialRepoKpi = RepoKpi(
      name_with_owner = Some("owner/repo"),
      name = Some("repo"),
      owner = Some("owner"),
      commits_count = Some(1L),
      contributors_count = Some(1L),
      best_contributor_login = Some("alice"),
      committers_map = Some(Map("alice" -> 1L)),
      ggazer_score = Some(2.0)
    )

    val pushEvent = PushEvent(
      event = GitHubEvent(
        id = "2",
        actor_login = "alice",
        repo_name = "owner/repo",
        event_type = "PushEvent",
        created_at = "2026-01-06T12:01:00Z"
      ),
      ref = None
    )
    val enrichedEvent = EnrichedEvent(
      event = Some(pushEvent),
      repo = Some(Repo(
        name_with_owner = Some("owner/repo"),
        owner = Some("owner"),
        name = Some("repo"),
        description = None,
        created_at = None,
        disk_usage = None,
        visibility = None,
        stargazers_count = None,
        forks_count = None,
        watchers_count = None,
        issues_count = None,
        primary_language = None
      )),
      actor = Some(Actor(
        login = "alice",
        name = None,
        email = None,
        bio = None,
        company = None,
        location = None,
        websiteUrl = None,
        `type` = None
      ))
    )

    val result = aggregate("owner/repo", enrichedEvent, initialRepoKpi)
    result.commits_count shouldBe Some(2L)
    result.contributors_count shouldBe Some(1L)
    result.committers_map shouldBe Some(Map("alice" -> 2L))
    result.best_contributor_login shouldBe Some("alice")
    result.ggazer_score shouldBe Some(2.5)
  }

  it should "add a new contributor and update best_contributor_login if needed" in {
    val initialRepoKpi = RepoKpi(
      name_with_owner = Some("owner/repo"),
      name = Some("repo"),
      owner = Some("owner"),
      commits_count = Some(2L),
      contributors_count = Some(1L),
      best_contributor_login = Some("alice"),
      committers_map = Some(Map("alice" -> 2L)),
      ggazer_score = Some(2.5)
    )

    val pushEvent = PushEvent(
      event = GitHubEvent(
        id = "3",
        actor_login = "bob",
        repo_name = "owner/repo",
        event_type = "PushEvent",
        created_at = "2026-01-06T12:02:00Z"
      ),
      ref = None
    )
    val enrichedEvent = EnrichedEvent(
      event = Some(pushEvent),
      repo = Some(Repo(
        name_with_owner = Some("owner/repo"),
        owner = Some("owner"),
        name = Some("repo"),
        description = None,
        created_at = None,
        disk_usage = None,
        visibility = None,
        stargazers_count = None,
        forks_count = None,
        watchers_count = None,
        issues_count = None,
        primary_language = None
      )),
      actor = Some(Actor(
        login = "bob",
        name = None,
        email = None,
        bio = None,
        company = None,
        location = None,
        websiteUrl = None,
        `type` = None
      ))
    )

    val result = aggregate("owner/repo", enrichedEvent, initialRepoKpi)
    result.commits_count shouldBe Some(3L)
    result.contributors_count shouldBe Some(2L)
    result.committers_map shouldBe Some(Map("alice" -> 2L, "bob" -> 1L))
    result.best_contributor_login shouldBe Some("alice")
    result.ggazer_score shouldBe Some(4.5)
  }
}
