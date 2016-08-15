package mesosphere.marathon.integration

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.integration.facades.ITEnrichedTask
import mesosphere.marathon.integration.setup._
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }

/**
  * Integration test to simulate the issues discovered a verizon where a network partition caused Marathon to be
  * separated from ZK and the leading Master.  During this separation the agents were partitioned from the Master.  Marathon was
  * bounced, then the network connectivity was re-established.  At which time the Mesos kills tasks on the slaves and marathon never
  * restarts them.
  *
  * This collection of integration tests is intended to go beyond the experience at Verizon.  The network partition in these tests
  * are simulated with a disconnection from the processes.
  */
class NetworkPartitionIntegrationTest extends IntegrationFunSuite
    with EmbeddedMarathonMesosClusterTest with Matchers
    with GivenWhenThen with BeforeAndAfter with StrictLogging {

  //override to start marathon with a low reconciliation frequency
  override val marathonArgs: Map[String, String] = Map(
    "reconciliation_initial_delay" -> "5000",
    "reconciliation_interval" -> "5000",
    "scale_apps_initial_delay" -> "5000",
    "scale_apps_interval" -> "5000",
    "min_revive_offers_interval" -> "100")

  before {
    cleanUp()
  }

  test("Loss of ZK and Loss of Slave will not kill the task when slave comes back") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)
    marathon.createAppV2(app)
    waitForEvent("deployment_success")
    val task = waitForTasks(app.id, 1).head

    When("We stop the slave, the task is declared lost")
    mesosCluster.agents.head.stop()

    waitForEventMatching("Task is declared lost") {
      matchEvent("TASK_LOST", task)
    }

    And("The task is shows in marathon as lost")
    val lost = waitForTasks(app.id, 1).head
    lost.state should be("TASK_LOST")

    When("the master bounds and the slave starts again")
    // network partition of zk
    zkServer.stop()
    mesosCluster.masters.head.stop()

    // zk back in service
    zkServer.start()

    // bring up the cluster
    mesosCluster.masters.head.start()
    mesosCluster.agents.head.start()

    Then("The task reappears as running")
    waitForEventMatching("Task is declared running again") {
      matchEvent("TASK_RUNNING", task)
    }
  }

  def matchEvent(status: String, task: ITEnrichedTask): CallbackEvent => Boolean = { event =>
    event.info.get("taskStatus").contains(status) &&
      event.info.get("taskId").contains(task.id)
  }
}
