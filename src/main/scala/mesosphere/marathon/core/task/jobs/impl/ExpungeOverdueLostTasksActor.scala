package mesosphere.marathon
package core.task.jobs.impl

import akka.actor.{ Actor, ActorLogging, Cancellable, Props }
import akka.event.LoggingAdapter
import akka.pattern.pipe
import java.util.concurrent.TimeUnit

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.jobs.TaskJobsConfig
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.core.task.tracker.InstanceTracker.SpecInstances
import mesosphere.marathon.state.{ PathId, Timestamp }
import org.apache.mesos.Protos.TaskStatus

import scala.concurrent.duration.FiniteDuration

/**
  * Business logic of overdue tasks actor.
  *
  * Factoring out into a trait makes testing simpler.
  */
trait ExpungeOverdueLostTasksActorLogic {

  import Timestamp._

  def log: LoggingAdapter
  val config: TaskJobsConfig
  val clock: Clock
  val stateOpProcessor: TaskStateOpProcessor

  // Timeouts will be configurable. See MARATHON-1228 and MARATHON-1227
  lazy val timeUntilReplacement = config.taskLostExpungeGC / 2
  lazy val timeUntilExpunge = config.taskLostExpungeGC / 2

  def triggerExpunge(instance: Instance): Unit = {
    val since = instance.state.since
    log.warning(s"Instance ${instance.instanceId} is unreachable since $since and will be expunged.")
    val stateOp = InstanceUpdateOperation.ForceExpunge(instance.instanceId)
    stateOpProcessor.process(stateOp)
  }

  def triggerUpdate(now: Timestamp)(instance: Instance): Unit = {
    // Trigger Instance.update by resending an identical update.
    // In the future we want to ask Mesos to give us an update and note fake as we do here.
    // See MARATHON-1239
    val expiredTasks = instance.tasksMap.values.filter(withExpiredUnreachableStatus(now))

    assert(expiredTasks.nonEmpty)
    assert(expiredTasks.head.mesosStatus.isDefined)

    val mesosStatus = expiredTasks.head.mesosStatus.get

    val stateOp = InstanceUpdateOperation.MesosUpdate(instance, mesosStatus, now)
    stateOpProcessor.process(stateOp)
  }

  /**
    * @return true if task has an unreachable status that is expired.
    */
  def isExpired(status: TaskStatus, now: Timestamp, timeout: FiniteDuration): Boolean = {
    val since: Timestamp =
      if (status.hasUnreachableTime) status.getUnreachableTime
      else Timestamp(TimeUnit.MICROSECONDS.toMillis(status.getTimestamp.toLong))
    since.expired(now, by = timeout)
  }

  /**
    * @return true if task has an UnreachableInactive status that is [[timeUntilExpunge]]
    *         millis older than now.
    */
  def withExpiredUnreachableInactiveStatus(now: Timestamp)(task: Task): Boolean = {
    // A task becomes UnreachableIntactive when status.getUnreachableTime is older than timeUntilReplacement.
    // A task will be expunged when it has been UnreachableIntactive for more than timeUntilExpunge.
    // Thus a task should be expunged if its getUnreachableTime is older than timeUntilReplacement + timeUntilExpunge.
    val totalTimeout = timeUntilReplacement + timeUntilExpunge
    task.mesosStatus.fold(false)(status => isExpired(status, now, totalTimeout))
  }

  /**
    * @return true if task has an Unreachable status that is half of [[timeUntilReplacement]]
    *         millis older than now.
    */
  def withExpiredUnreachableStatus(now: Timestamp)(task: Task): Boolean =
    task.mesosStatus.fold(false)(status => isExpired(status, now, timeUntilReplacement))

  /**
    * @return instances that have been Unreachable for more than [[timeUntilReplacement]] millis.
    */
  def filterOverdueUnreachable(instances: Map[PathId, SpecInstances], now: Timestamp) =
    instances.values.flatMap(_.instances)
      .withFilter(_.isUnreachable)
      .withFilter(_.tasks.exists(withExpiredUnreachableStatus(now)))

  /**
    * @return instances that have been UnreachableInactive for more than half of [[mesosphere.marathon.core.task.jobs.TaskJobsConfig.taskLostExpungeGC]] millis.
    */
  def filterOverdueUnreachableInactive(instances: Map[PathId, SpecInstances], now: Timestamp) =
    instances.values.flatMap(_.instances)
      .withFilter(_.isUnreachableInactive)
      .withFilter(_.tasks.exists(withExpiredUnreachableInactiveStatus(now)))
}

case class ExpungeOverdueLostTasksActor(
    clock: Clock,
    config: TaskJobsConfig,
    instanceTracker: InstanceTracker,
    stateOpProcessor: TaskStateOpProcessor) extends Actor with ActorLogging with ExpungeOverdueLostTasksActorLogic {

  import ExpungeOverdueLostTasksActor._
  implicit val ec = context.dispatcher

  var tickTimer: Option[Cancellable] = None

  override def preStart(): Unit = {
    log.info("ExpungeOverdueLostTasksActor has started")
    tickTimer = Some(context.system.scheduler.schedule(
      config.taskLostExpungeInitialDelay,
      config.taskLostExpungeInterval, self, Tick))
  }

  override def postStop(): Unit = {
    tickTimer.foreach(_.cancel())
    log.info("ExpungeOverdueLostTasksActor has stopped")
  }

  override def receive: Receive = {
    case Tick => instanceTracker.instancesBySpec() pipeTo self
    case InstanceTracker.InstancesBySpec(instances) =>
      filterOverdueUnreachable(instances, clock.now()).foreach(triggerUpdate(clock.now()))
      filterOverdueUnreachableInactive(instances, clock.now()).foreach(triggerExpunge)
  }
}

object ExpungeOverdueLostTasksActor {

  case object Tick

  def props(clock: Clock, config: TaskJobsConfig,
    instanceTracker: InstanceTracker, stateOpProcessor: TaskStateOpProcessor): Props = {
    Props(new ExpungeOverdueLostTasksActor(clock, config, instanceTracker, stateOpProcessor))
  }
}
