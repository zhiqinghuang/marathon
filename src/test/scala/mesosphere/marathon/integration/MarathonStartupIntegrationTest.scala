package mesosphere.marathon.integration

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.integration.setup._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.ScalaFutures

class MarathonStartupIntegrationTest extends IntegrationFunSuite
    with EmbeddedMarathonTest
    with GivenWhenThen
    with ScalaFutures
    with StrictLogging {

  test("Marathon should fail during start, if the HTTP port is already bound") {
    Given(s"a Marathon process already running on port ${marathonServer.httpPort}")

    When("starting another Marathon process using an HTTP port that is already bound")

    val conflict = LocalMarathon(false, marathonServer.masterUrl, marathonServer.zkUrl,
      conf = Map("http_port" -> marathonServer.httpPort.toString))

    Then("An uncaught exception should be thrown")
    intercept[Throwable] {
      conflict.marathon.start()
    }
    conflict.close()
  }
}
