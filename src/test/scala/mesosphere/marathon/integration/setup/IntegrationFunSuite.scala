package mesosphere.marathon.integration.setup

import mesosphere.marathon.state.PathId
import org.joda.time.DateTime
import org.scalatest._

import scala.concurrent.duration._

/**
  * All integration tests should be marked with this tag.
  * Integration tests need a special set up and can take a long time.
  * So it is not desirable, that these kind of tests run every time all the unit tests run.
  */
object IntegrationTag extends Tag("mesosphere.marathon.IntegrationTest")

/**
  * Health check helper to define health behaviour of launched applications
  */
class IntegrationHealthCheck(val appId: PathId, val versionId: String, val port: Int, var state: Boolean, var lastUpdate: DateTime = DateTime.now) {

  case class HealthStatusChange(deadLine: Deadline, state: Boolean)
  private[this] var changes = List.empty[HealthStatusChange]
  private[this] var healthAction = (check: IntegrationHealthCheck) => {}
  var pinged = false

  def afterDelay(delay: FiniteDuration, state: Boolean): Unit = {
    val item = HealthStatusChange(delay.fromNow, state)
    def insert(ag: List[HealthStatusChange]): List[HealthStatusChange] = {
      if (ag.isEmpty || item.deadLine < ag.head.deadLine) item :: ag
      else ag.head :: insert(ag.tail)
    }
    changes = insert(changes)
  }

  def withHealthAction(fn: (IntegrationHealthCheck) => Unit): this.type = {
    healthAction = fn
    this
  }

  def healthy: Boolean = {
    healthAction(this)
    pinged = true
    val (past, future) = changes.partition(_.deadLine.isOverdue())
    state = past.lastOption.fold(state)(_.state)
    changes = future
    lastUpdate = DateTime.now()
    state
  }

  def forVersion(versionId: String, state: Boolean) = {
    val result = new IntegrationHealthCheck(appId, versionId, port, state)
    result
  }

  def pingSince(duration: Duration): Boolean = DateTime.now.minusMillis(duration.toMillis.toInt).isBefore(lastUpdate)
}

