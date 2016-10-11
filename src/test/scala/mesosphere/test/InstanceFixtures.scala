package mesosphere.test

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.bus.TaskStatusUpdateTestHelper
import mesosphere.marathon.state.RunSpec
import mesosphere.test.InstanceFixtures.{ StatusUpdate, TaskUpdateStatus }
import org.apache.mesos.Protos.TaskState
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

trait InstanceFixtures {
}

object InstanceFixtures {

  sealed trait TaskUpdateStatus {
    def taskState: TaskState
  }
  object TaskUpdateStatus {
    case object Starting extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_STARTING
    }
    case object Staging extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_STAGING
    }
    case object Running extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_RUNNING
    }
    case object RunningAndHealthy extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_RUNNING
    }
    case object RunningAndUnhealthy extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_RUNNING
    }
    case object Error extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_ERROR
    }
    case object Failed extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_FAILED
    }
    case object Finished extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_FINISHED
    }
    case object Killed extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_KILLED
    }
    case object Killing extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_KILLING
    }
    case object Unreachable extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_UNREACHABLE
    }
    case object Gone extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_GONE
    }
    case object Dropped extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_DROPPED
    }
    case object Unknown extends TaskUpdateStatus {
      override def taskState: TaskState = TaskState.TASK_UNKNOWN
    }
  }

  case class StatusUpdate(task: Task, status: TaskUpdateStatus)
}

object TestConversions {
  private[this] val instanceBuilder = new TestInstanceBuilder()

  implicit class RunSpecAdditions(val runSpec: RunSpec) extends AnyVal {
    def buildInstance(): Instance = instanceBuilder.buildFrom(runSpec)
  }

  implicit class InstanceAdditions(val instance: Instance) extends AnyVal {
    def taskAt(index: Int): Task = {
      try {
        instance.tasks.drop(index).head
      } catch {
        case e: NoSuchElementException =>
          throw new RuntimeException(s"no task at index $index in $instance")
      }
    }

    /**
      * Apply a given list of status updates to an instance and return the updated instance.
      * @param updates A list of tuples of a Task and the InstanceStatus it shall be updated to
      * @return The instance after all updates have been applied.
      */
    def update(updates: (Task, TaskUpdateStatus)*): Instance = {
      @tailrec
      def loop(instance: Instance, updates: List[StatusUpdate]): Instance = updates match {
        case update :: tail =>
          val updatedInstance = Updates.statusUpdate(instance, update.task, update.status)
          loop(updatedInstance, tail)
        case Nil => instance
      }

      val statusUpdates: List[StatusUpdate] = updates.map(StatusUpdate.tupled)(collection.breakOut)
      loop(instance, statusUpdates)
    }
  }

  implicit class TupleConversion(val tuple: (Task, TaskUpdateStatus)) extends AnyVal {
    def toStatusUpdate: StatusUpdate = StatusUpdate(tuple._1, tuple._2)
  }
}

object Updates extends Clocked {
  import org.apache.mesos.Protos.TaskStatus

  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] def update(instance: Instance, mesosStatus: TaskStatus): Instance = {
    val operation = InstanceUpdateOperation.MesosUpdate(
      instance = instance,
      mesosStatus = mesosStatus,
      now = tick()
    )
    instance.update(operation) match {
      case update: InstanceUpdateEffect.Update => update.instance
      case expunge: InstanceUpdateEffect.Expunge => expunge.instance
      case noop: InstanceUpdateEffect.Noop =>
        log.warn(s"$operation results in a Noop - possibly unexpected")
        instance
      case failure: InstanceUpdateEffect.Failure =>
        throw new RuntimeException(s"$operation results in a Failure: $failure")
    }
  }

  def statusUpdate(instance: Instance, task: Task, status: TaskUpdateStatus): Instance = {
    val maybeHealth = status match {
      case TaskUpdateStatus.RunningAndHealthy => Some(true)
      case TaskUpdateStatus.RunningAndUnhealthy => Some(false)
      case _ => None
    }
    update(
      instance = instance,
      mesosStatus = TaskStatusUpdateTestHelper.makeTaskStatus(task.taskId, status.taskState, maybeHealth))
  }

}
