package mesosphere.test

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.launcher.InstanceOpFactory
import mesosphere.marathon.core.launcher.impl.InstanceOpFactoryImpl
import mesosphere.marathon.state.RunSpec
import mesosphere.marathon.test.MarathonTestHelper

class TestInstanceBuilder() extends Clocked {
  private[this] val config = MarathonTestHelper.defaultConfig()
  private[this] val opFactory = new InstanceOpFactoryImpl(config)(clock)
  private[this] val sufficientOffer = MarathonTestHelper.makeBasicOffer(
    cpus = 100,
    mem = 100000,
    disk = 100000
  ).build()

  def buildFrom(runSpec: RunSpec): Instance = {
    val maybeOp = opFactory.buildTaskOp(InstanceOpFactory.Request(
      runSpec,
      sufficientOffer,
      instanceMap = Map.empty[Instance.Id, Instance],
      additionalLaunches = 1))
    val maybeInstance = maybeOp.map { op =>
      op.stateOp match {
        case InstanceUpdateOperation.LaunchEphemeral(instance) => instance
        case _ => throw new RuntimeException(s"can't map ${op.stateOp} to an instance")
      }
    }

    maybeInstance.getOrElse(throw new RuntimeException("buildTaskOpFailed"))
  }
}
