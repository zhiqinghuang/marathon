package mesosphere.marathon.integration

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.setup.{ EmbeddedMarathonTest, IntegrationFunSuite }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }

class TaskKillingIntegrationTest extends IntegrationFunSuite
    with EmbeddedMarathonTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen
    with ScalaFutures
    with StrictLogging {

  override val marathonArgs: Map[String, String] = Map("enable_features" -> "task_killing")

  test("Killing a task publishes a TASK_KILLING event") {
    Given("a new app")
    val app = appProxy(testBasePath / "app", "v1", instances = 1, withHealth = false)

    When("The app is deployed")
    val createResult = marathon.createAppV2(app)

    Then("The app is created")
    createResult.code should be (201) //Created
    extractDeploymentIds(createResult) should have size 1
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1) //make sure, the app has really started

    When("the task is killed")
    val killResult = marathon.killAllTasksAndScale(app.id)
    killResult.code should be (200) //OK
    waitForStatusUpdates("TASK_KILLING")
  }

}
