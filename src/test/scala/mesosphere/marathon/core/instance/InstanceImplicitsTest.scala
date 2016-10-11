package mesosphere.marathon.core.instance

import mesosphere.marathon.core.pod.{MesosContainer, PodDefinition}
import mesosphere.UnitTest
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state.PathId
import mesosphere.test.InstanceFixtures.TaskUpdateStatus
import scala.collection.immutable.Seq

class InstanceImplicitsTest extends UnitTest {
  import mesosphere.test.TestConversions._

  "InstanceFixtures" should {
    "be super easy to use" in {
      val runSpec = PodDefinition(
        id = PathId("/test-pod"),
        containers = Seq(
          MesosContainer(
            name = "container1",
            resources = Resources()
          ),
          MesosContainer(
            name = "container2",
            resources = Resources()
          )
        )
      )

      // create an instance from a runSpec
      var instance = runSpec.buildInstance()
      instance.isCreated shouldBe true

      // apply a list of status update to a given tasks
      instance = instance.update(
        instance.taskAt(0) -> TaskUpdateStatus.Staging,
        instance.taskAt(1) -> TaskUpdateStatus.Staging,
        instance.taskAt(0) -> TaskUpdateStatus.Finished,
        instance.taskAt(1) -> TaskUpdateStatus.Running
      )

      instance.taskAt(0).isFinished shouldBe true
      instance.taskAt(1).isRunning shouldBe true
      instance.isRunning shouldBe true
    }
  }

}
