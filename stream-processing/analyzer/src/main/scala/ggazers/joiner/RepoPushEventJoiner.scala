package ggazers.joiner
import ggazers.avro.message.{Repo, PushEvent}


trait RepoPushEventJoiner {
  def joinPushEventWithRepo: (Repo, PushEvent) => (Repo, Long) = {
    (repo: Repo, pushEvent: PushEvent) => {
      val updatedRepo = Repo(
        name = repo.name,
        full_name = pushEvent.event.repo_name,
        owner = pushEvent.event.repo_owner.getOrElse(""),
        private_repo = false
      )
      (repo, 1L)
    }
  }
}