package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationTest
import mesosphere.marathon.test.ExitDisabledTest

import scala.util.Try

@IntegrationTest
class MarathonCommandLineHelpTest extends AkkaIntegrationTest with ExitDisabledTest {
  "Marathon" when {
    "passed --help shouldn't crash" in {
      Try(new MarathonApp(Seq("--help")))
      exitCalled(0).futureValue should be(true)
    }
  }
}
