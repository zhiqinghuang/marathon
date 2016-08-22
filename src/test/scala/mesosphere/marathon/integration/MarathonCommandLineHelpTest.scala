package mesosphere.marathon.integration

import mesosphere.marathon.MarathonApp
import mesosphere.marathon.integration.setup.{ ExitDisabledTest, IntegrationFunSuite }
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }

import scala.collection.immutable.Seq

class MarathonCommandLineHelpTest
    extends IntegrationFunSuite
    with ExitDisabledTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  test("marathon --help shouldn't crash") {
    intercept[IllegalStateException] {
      val app = new MarathonApp(Seq("--help"))
      app.start()
    }.getMessage should equal("Attempted to call exit with code: 0")
    exitsCalled(_.exists(_ == 0)) should be(true)
  }
}
