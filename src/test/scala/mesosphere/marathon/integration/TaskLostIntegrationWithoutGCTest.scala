package mesosphere.marathon.integration

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.Protos
import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.marathon.api.v2.json.AppUpdate
import mesosphere.marathon.integration.facades.ITEnrichedTask
import mesosphere.marathon.integration.setup._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }

class TaskLostIntegrationWithoutGCTest extends IntegrationFunSuite with EmbeddedMarathonMesosClusterTest
    with Matchers with GivenWhenThen with BeforeAndAfter with ScalaFutures with StrictLogging {

  override lazy val mesosNumMasters = 2
  override lazy val mesosNumSlaves = 2

  override val marathonArgs: Map[String, String] = Map(
    "reconciliation_initial_delay" -> "5000",
    "reconciliation_interval" -> "5000",
    "scale_apps_initial_delay" -> "5000",
    "scale_apps_interval" -> "5000",
    "min_revive_offers_interval" -> "100",
    "task_lost_expunge_gc" -> "300000000",
    "task_lost_expunge_initial_delay" -> "1000000",
    "task_lost_expunge_interval" -> "60000"
  )

  before {
    cleanUp()
    mesosCluster.agents.tail.head.stop()
    mesosCluster.masters.tail.head.stop()
    mesosCluster.masters.head.start()
    mesosCluster.agents.head.start()
  }

  test("A task lost with mesos master failover will not expunge the task and a mesosphere.marathon.scale down will succeed") {
    // TODO: the test should also run with 1 task and one agent
    mesosCluster.agents.tail.head.start()

    Given("a new app")
    val appId = testBasePath / "app"
    val app = appProxy(appId, "v1", instances = 2, withHealth = false).copy(
      cmd = Some("sleep 1000"),
      constraints = Set(
        // make sure each agent runs one task so that no task is launched after one agent goes down
        Protos.Constraint.newBuilder().setField("hostname").setOperator(Operator.UNIQUE).build())
    )
    marathon.createAppV2(app)

    When("the deployment is finished")
    waitForEvent("deployment_success")
    val tasks0 = waitForTasks(app.id, 2)

    Then("there are 2 running tasks on 2 agents")
    tasks0 should have size 2
    tasks0.forall(_.state == "TASK_RUNNING") shouldBe true
    val task = tasks0.find(_.host == "0").getOrElse(fail("no task was started on slave1"))
    tasks0.find(_.host == "1").getOrElse(fail("no task was started on slave2"))

    When("We stop one agent, one task is declared lost")
    mesosCluster.agents.head.stop()
    waitForEventMatching("Task is declared lost") { matchEvent("TASK_LOST", task) }

    And("The task is NOT removed from the task list")
    val tasks1 = waitForTasks(app.id, 2)
    tasks1 should have size 2
    tasks1.exists(_.state == "TASK_LOST") shouldBe true

    When("We mesosphere.marathon.scale down the app")
    marathon.updateApp(appId, AppUpdate(instances = Some(1)))

    Then("The deployment finishes")
    waitForEvent("deployment_success")

    And("The lost task is expunged")
    val tasks2 = marathon.tasks(app.id).value
    tasks2 should have size 1
    tasks2.exists(_.state == "TASK_RUNNING") shouldBe true
  }

  def matchEvent(status: String, task: ITEnrichedTask): CallbackEvent => Boolean = { event =>
    event.info.get("taskStatus").contains(status) &&
      event.info.get("taskId").contains(task.id)
  }

}
