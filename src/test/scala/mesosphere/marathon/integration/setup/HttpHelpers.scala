package mesosphere.marathon.integration.setup

import play.api.libs.json.{ JsValue, Json }
import spray.http.HttpResponse

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Awaitable }

/**
  * Result of an REST operation.
  */
case class RestResult[+T](valueGetter: () => T, originalResponse: HttpResponse) {
  def code: Int = originalResponse.status.intValue
  def success: Boolean = code == 200
  lazy val value: T = valueGetter()

  /** Transform the value of this result. */
  def map[R](change: T => R): RestResult[R] = {
    RestResult(() => change(valueGetter()), originalResponse)
  }

  /** Display the original response entity (=body) as string. */
  lazy val entityString: String = originalResponse.entity.asString

  /** Parse the original response entity (=body) as json. */
  lazy val entityJson: JsValue = Json.parse(entityString)

  /** Pretty print the original response entity (=body) as json. */
  lazy val entityPrettyJsonString: String = Json.prettyPrint(entityJson)
}

object RestResult {
  def apply(response: HttpResponse): RestResult[HttpResponse] = {
    new RestResult[HttpResponse](() => response, response)
  }

  def await(responseFuture: Awaitable[HttpResponse], waitTime: Duration): RestResult[HttpResponse] = {
    apply(Await.result(responseFuture, waitTime))
  }
}

/**
  * The common data structure for all callback events.
  * Needed for dumb jackson.
  */
case class CallbackEvent(eventType: String, info: Map[String, Any])

object UpdateEventsHelper {
  implicit class CallbackEventToStatusUpdateEvent(val event: CallbackEvent) extends AnyVal {
    def taskStatus: String = event.info("taskStatus").toString
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
