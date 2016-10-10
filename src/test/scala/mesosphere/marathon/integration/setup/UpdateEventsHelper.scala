package mesosphere.marathon.integration.setup

object UpdateEventsHelper {
  implicit class CallbackEventToStatusUpdateEvent(val event: CallbackEvent) extends AnyVal {
    def taskStatus: String = event.info.get("taskStatus").map(_.toString).getOrElse("")
    def message: String = event.info("message").toString
    def id: String = event.info("id").toString
    def running: Boolean = taskStatus == "TASK_RUNNING"
    def finished: Boolean = taskStatus == "TASK_FINISHED"
    def failed: Boolean = taskStatus == "TASK_FAILED"
  }

  object StatusUpdateEvent {
    def unapply(event: CallbackEvent): Option[CallbackEvent] = {
      if (event.eventType == "status_update_event") Some(event)
      else None
    }
  }
}
