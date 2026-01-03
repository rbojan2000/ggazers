package ggazers.joiner

import ggazers.avro.message.{Actor, PushEvent, ActorPushEvent}

trait UserPushEventJoiner {
  def joinUserWithPushEvent: (Actor, PushEvent) => ActorPushEvent = {
    (actor: Actor, pushEvent: PushEvent) => {

// ActorPushEvent(var login: String, var repo_full_name: String, var repo_name: String, var repo_owner: String, var repo_owner_actor_type: String) extends org.apache.avro.specific.SpecificRecordBase {
// Actor(var login: String, var name: Option[String], var email: Option[String], var bio: Option[String], var company: Option[String], var location: Option[String], var websiteUrl: Option[String], var `type`: Option[String] = None) extends org.apache.avro.specific.SpecificRecordBase {
// GitHubEvent(var id: String, var actor_login: String, var repo_name: String, var repo_short_name: Option[String] = None, var repo_owner: Option[String] = None, var event_type: String, var created_at: String) extends org.apache.avro.specific.SpecificRecordBase {
// PushEvent(var event: ggazers.avro.message.GitHubEvent, var ref: Option[String] = None) extends org.apache.avro.specific.SpecificRecordBase {

      ActorPushEvent(
        login = actor.login,
        repo_full_name = pushEvent.event.repo_name,
        repo_name = pushEvent.event.repo_short_name,
        repo_owner = pushEvent.event.repo_owner,


      )
    }
  }
}