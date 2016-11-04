package mesosphere.marathon
package core.task

import java.util.Base64

import com.fasterxml.uuid.{ EthernetAddress, Generators }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.condition.Condition.Terminal
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.core.task.Task.Reservation.Timeout.Reason.{ RelaunchEscalationTimeout, ReservationTimeout }
import mesosphere.marathon.core.task.update.{ TaskUpdateEffect, TaskUpdateOperation }
import mesosphere.marathon.state.{ AppDefinition, PathId, PersistentVolume, RunSpec, Timestamp }
import mesosphere.marathon.stream._
import org.apache.mesos
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.Protos.{ TaskState, TaskStatus }
import org.apache.mesos.{ Protos => MesosProtos }
import org.slf4j.LoggerFactory
// TODO PODS remove api imports
import mesosphere.marathon.api.v2.json.Formats._
import play.api.libs.functional.syntax._
import play.api.libs.json._

/**
  * The state for launching a task. This might be a launched task or a reservation for launching a task or both.
  *
  * <pre>
  * +-----------------------+ +---------------------------------------+
  * +EPHEMERAL              + +               RESIDENT                |
  * +-----------------------+ +---------------------------------------+
  *                            _  _
  *   | match offer      ___ (~ )( ~)                  | match
  *   | & launch        /   \_\ \/ /                   | offer
  *   |                |   D_ ]\ \/                    | & reserve
  *   |            +-> |   D _]/\ \   <-+              |
  *   |   terminal |    \___/ / /\ \    | confirmed(?) |
  *   |     status |         (_ )( _)   | unreserve /  |
  *   v     update |       DELETED      | expunge      v
  *                |                    |
  * +--------------+--------+ +---------+---------------+
  * | LaunchedEphemeral     | | Reserved                |
  * |                       | |                         |
  * |  Started / Staged     | |  New / Launched         |
  * |  Running              | |  Suspended / Garbage    |
  * |                       | |  Unknown                | <-+
  * +-----------------------+ +-------------------------+   |
  *                           |                             |
  *                           |   +---------------------+   |
  *               match offer |   |LaunchedOnReservation|   | terminal
  *               & launch    |   |                     |   | status
  *                           |   | Started / Staged    |   | update
  *                           |   | Running             |   |
  *                           +-> |                     +---+
  *                               +---------------------+
  *
  * </pre>
  *
  * Note on wiping reserved tasks: It is not fully implemented right now so that
  * LaunchedOnReservation tasks might get immediately expunged right now.
  * Marathon will notice spurious tasks in the offer and create the appropriate
  * unreserve operations. See https://github.com/mesosphere/marathon/issues/3223
  */
sealed trait Task {
  def taskId: Task.Id
  def agentInfo: Instance.AgentInfo
  def reservationWithVolumes: Option[Task.Reservation]
  def launched: Option[Task.Launched]
  def runSpecVersion: Timestamp

  /** apply the given operation to a task */
  def update(update: TaskUpdateOperation): TaskUpdateEffect

  def runSpecId: PathId = taskId.runSpecId

  def status: Task.Status

  def launchedMesosId: Option[MesosProtos.TaskID] = launched.map { _ =>
    // it doesn't make sense for an unlaunched task
    taskId.mesosTaskId
  }

  // TODO: remove this method (DCOS-10332)
  def mesosStatus: Option[MesosProtos.TaskStatus] = {
    status.mesosStatus.orElse {
      launchedMesosId.map { mesosId =>
        val taskStatusBuilder = MesosProtos.TaskStatus.newBuilder
          .setState(TaskState.TASK_STAGING)
          .setTaskId(mesosId)

        agentInfo.agentId.foreach { slaveId =>
          taskStatusBuilder.setSlaveId(MesosProtos.SlaveID.newBuilder().setValue(slaveId))
        }

        taskStatusBuilder.build()
      }
    }
  }

  def effectiveIpAddress(runSpec: RunSpec): Option[String] =
    runSpec match {
      case app: AppDefinition if app.usesNonHostNetworking =>
        status.ipAddresses.flatMap(_.headOption).map(_.getIpAddress)

      // TODO(PODS) extract ip address from launched task
      case _ =>
        Some(agentInfo.host)
    }
}

object Task {

  def unapply(task: Task): Option[(Option[Task.Launched], Option[MesosProtos.TaskStatus])] = Some((task.launched, task.mesosStatus))

  // TODO PODs remove api import
  import mesosphere.marathon.api.v2.json.Formats.PathIdFormat

  case class Id(idString: String) extends Ordered[Id] {
    lazy val mesosTaskId: MesosProtos.TaskID = MesosProtos.TaskID.newBuilder().setValue(idString).build()
    lazy val runSpecId: PathId = Id.runSpecId(idString)
    lazy val instanceId: Instance.Id = Id.instanceId(idString)
    lazy val containerName: Option[String] = Id.containerName(idString)
    override def toString: String = s"task [$idString]"
    override def compare(that: Id): Int = idString.compare(that.idString)
  }

  object Id {

    @SuppressWarnings(Array("LooksLikeInterpolatedString"))
    object Names {
      val anonymousContainer = "$anon" // presence of `$` is important since it's illegal for a real container name!
    }
    // Regular expression for matching taskIds before instance-era
    private val LegacyTaskIdRegex = """^(.+)[\._]([^_\.]+)$""".r

    // Regular expression for matching taskIds since instance-era
    private val TaskIdWithInstanceIdRegex = """^(.+)\.(instance-|marathon-)([^_\.]+)[\._]([^_\.]+)$""".r

    private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())

    def runSpecId(taskId: String): PathId = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceId, maybeContainer) => PathId.fromSafePath(runSpecId)
        case LegacyTaskIdRegex(runSpecId, uuid) => PathId.fromSafePath(runSpecId)
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def containerName(taskId: String): Option[String] = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceUuid, maybeContainer) =>
          if (maybeContainer == Names.anonymousContainer) None else Some(maybeContainer)
        case LegacyTaskIdRegex(runSpecId, uuid) => None
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def instanceId(taskId: String): Instance.Id = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceUuid, uuid) =>
          Instance.Id(runSpecId + "." + prefix + instanceUuid)
        case LegacyTaskIdRegex(runSpecId, uuid) =>
          Instance.Id(runSpecId + "." + calculateLegacyExecutorId(uuid))
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def apply(mesosTaskId: MesosProtos.TaskID): Id = new Id(mesosTaskId.getValue)

    def forRunSpec(id: PathId): Id = {
      val taskId = id.safePath + "." + uuidGenerator.generate()
      Task.Id(taskId)
    }

    def forInstanceId(instanceId: Instance.Id, container: Option[MesosContainer]): Id =
      Id(instanceId.idString + "." + container.map(c => c.name).getOrElse(Names.anonymousContainer))

    implicit val taskIdFormat = Format(
      Reads.of[String](Reads.minLength[String](3)).map(Task.Id(_)),
      Writes[Task.Id] { id => JsString(id.idString) }
    )

    // pre-instance-era executorId="marathon-$taskId" and compatibility reasons we need this calculation.
    // Should be removed as soon as no tasks without instance exists (tbd)
    def calculateLegacyExecutorId(taskId: String): String = s"marathon-$taskId"
  }

  case class LocalVolume(id: LocalVolumeId, persistentVolume: PersistentVolume)

  case class LocalVolumeId(runSpecId: PathId, containerPath: String, uuid: String) {
    import LocalVolumeId._
    lazy val idString = runSpecId.safePath + delimiter + containerPath + delimiter + uuid

    override def toString: String = s"LocalVolume [$idString]"
  }

  object LocalVolumeId {
    private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())
    private val delimiter = "#"
    private val LocalVolumeEncoderRE = s"^([^$delimiter]+)[$delimiter]([^$delimiter]+)[$delimiter]([^$delimiter]+)$$".r

    def apply(runSpecId: PathId, volume: PersistentVolume): LocalVolumeId =
      LocalVolumeId(runSpecId, volume.containerPath, uuidGenerator.generate().toString)

    def unapply(id: String): Option[(LocalVolumeId)] = id match {
      case LocalVolumeEncoderRE(runSpec, path, uuid) => Some(LocalVolumeId(PathId.fromSafePath(runSpec), path, uuid))
      case _ => None
    }

    implicit val localVolumeIdReader = (
      (__ \ "runSpecId").read[PathId] and
      (__ \ "containerPath").read[String] and
      (__ \ "uuid").read[String]
    )((id, path, uuid) => LocalVolumeId(id, path, uuid))

    implicit val localVolumeIdWriter = Writes[LocalVolumeId] { localVolumeId =>
      JsObject(Seq(
        "runSpecId" -> Json.toJson(localVolumeId.runSpecId),
        "containerPath" -> Json.toJson(localVolumeId.containerPath),
        "uuid" -> Json.toJson(localVolumeId.uuid),
        "persistenceId" -> Json.toJson(localVolumeId.idString)
      ))
    }
  }

  /**
    * Represents a task which has been launched (i.e. sent to Mesos for launching).
    *
    * @param hostPorts sequence of ports in the Mesos Agent allocated to the task
    */
  // TODO: hostPorts should ultimately move to Task.Status (DCOS-10332)
  case class Launched(hostPorts: Seq[Int])

  /**
    * Contains information about the status of a launched task including timestamps for important
    * state transitions.
    *
    * @param stagedAt Despite its name, stagedAt is set on task creation and before the TASK_STAGED notification from
    *                 Mesos. This is important because we periodically check for any tasks with an old stagedAt
    *                 timestamp and kill them (See KillOverdueTasksActor).
    */
  case class Status(
      stagedAt: Timestamp,
      startedAt: Option[Timestamp] = None,
      mesosStatus: Option[MesosProtos.TaskStatus] = None,
      condition: Condition) {

    /**
      * @return the health status reported by mesos for this task
      */
    def healthy: Option[Boolean] = mesosStatus.withFilter(_.hasHealthy).map(_.getHealthy)

    def ipAddresses: Option[Seq[MesosProtos.NetworkInfo.IPAddress]] = mesosStatus.flatMap(MesosStatus.ipAddresses)
  }

  object Status {
    implicit object MesosTaskStatusFormat extends Format[mesos.Protos.TaskStatus] {
      override def reads(json: JsValue): JsResult[mesos.Protos.TaskStatus] = {
        json.validate[String].map { base64 =>
          mesos.Protos.TaskStatus.parseFrom(Base64.getDecoder.decode(base64))
        }
      }

      override def writes(o: TaskStatus): JsValue = {
        JsString(Base64.getEncoder.encodeToString(o.toByteArray))
      }
    }
    implicit val statusFormat = Json.format[Status]
  }

  object Terminated {
    def isTerminated(state: TaskState): Boolean = state match {
      case TASK_ERROR | TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST => true
      case _ => false
    }

    def unapply(state: TaskState): Option[TaskState] = if (isTerminated(state)) Some(state) else None
  }

  object MesosStatus {
    def ipAddresses(mesosStatus: MesosProtos.TaskStatus): Option[Seq[MesosProtos.NetworkInfo.IPAddress]] = {
      if (mesosStatus.hasContainerStatus && mesosStatus.getContainerStatus.getNetworkInfosCount > 0)
        Some(
          mesosStatus.getContainerStatus.getNetworkInfosList.flatMap(_.getIpAddressesList)(collection.breakOut)
        )
      else None
    }
  }

  /**
    * A LaunchedEphemeral task is a stateless task that does not consume reserved resources or persistent volumes.
    */
  case class LaunchedEphemeral(
      taskId: Task.Id,
      agentInfo: Instance.AgentInfo,
      runSpecVersion: Timestamp,
      status: Status,
      hostPorts: Seq[Int]) extends Task {

    import LaunchedEphemeral.log

    override def reservationWithVolumes: Option[Reservation] = None

    // TODO: it doesn't really make sense to provide the hostPorts in this wrapper, does it? (DCOS-10332)
    override def launched: Option[Launched] = Some(Task.Launched(hostPorts))

    private[this] def hasStartedRunning: Boolean = status.startedAt.isDefined

    override def update(op: TaskUpdateOperation): TaskUpdateEffect = op match {
      // exceptional case: the task is already terminal. Don't transition in this case.
      // This might be because the task terminated (e.g. finished) before Marathon issues a kill Request
      // to Mesos. Mesos will likely send back a TASK_LOST status update, because the task is no longer
      // known in Mesos. We'll never want to transition from one terminal state to another as a terminal
      // state should already be distinct enough.
      // related to https://github.com/mesosphere/marathon/pull/4531
      case op: TaskUpdateOperation if this.isTerminal =>
        log.warn(s"received $op for terminal $taskId, ignoring")
        TaskUpdateEffect.Noop

      case TaskUpdateOperation.MesosUpdate(Condition.Running, mesosStatus, now) if !hasStartedRunning =>
        val updatedTask = copy(status = status.copy(
          mesosStatus = Some(mesosStatus),
          condition = Condition.Running,
          startedAt = Some(now)
        ))
        TaskUpdateEffect.Update(newState = updatedTask)

      // The Terminal extractor applies specific logic e.g. when an Unreachable task becomes Gone
      case TaskUpdateOperation.MesosUpdate(newStatus: Terminal, mesosStatus, _) =>
        val updated = copy(status = status.copy(
          mesosStatus = Some(mesosStatus),
          condition = newStatus))
        TaskUpdateEffect.Update(updated)

      case TaskUpdateOperation.MesosUpdate(newStatus, mesosStatus, _) =>
        // TODO(PODS): strange to use Condition here
        updatedHealthOrState(status.mesosStatus, mesosStatus).map { newTaskStatus =>
          val updatedTask = copy(status = status.copy(
            mesosStatus = Some(newTaskStatus),
            condition = newStatus
          ))
          // TODO(PODS): The instance needs to handle a terminal task via an Update here
          // Or should we use Expunge in case of a terminal update for resident tasks?
          TaskUpdateEffect.Update(newState = updatedTask)
        } getOrElse {
          log.debug("Ignoring status update for {}. Status did not change.", taskId)
          TaskUpdateEffect.Noop
        }
    }
  }

  object LaunchedEphemeral {
    private val log = LoggerFactory.getLogger(getClass)
    implicit val launchedEphemeralFormat = Json.format[LaunchedEphemeral]
  }

  /**
    * Represents a reservation for all resources that are needed for launching a task
    * and associated persistent local volumes.
    */
  case class Reservation(volumeIds: Seq[LocalVolumeId], state: Reservation.State)

  object Reservation {
    /**
      * A timeout that eventually leads to a state transition
      *
      * @param initiated When this timeout was setup
      * @param deadline When this timeout should become effective
      * @param reason The reason why this timeout was set up
      */
    case class Timeout(initiated: Timestamp, deadline: Timestamp, reason: Timeout.Reason)

    object Timeout {
      sealed trait Reason
      object Reason {
        /** A timeout because the task could not be relaunched */
        case object RelaunchEscalationTimeout extends Reason
        /** A timeout because we got no ack for reserved resources or persistent volumes */
        case object ReservationTimeout extends Reason
      }

      implicit object ReasonFormat extends Format[Timeout.Reason] {
        override def reads(json: JsValue): JsResult[Timeout.Reason] = {
          json.validate[String].map {
            case "RelaunchEscalationTimeout" => RelaunchEscalationTimeout
            case "ReservationTimeout" => ReservationTimeout
          }
        }

        override def writes(o: Timeout.Reason): JsValue = {
          JsString(o.toString)
        }
      }
      implicit val timeoutFormat = Json.format[Timeout]
    }
    sealed trait State extends Product with Serializable {
      /** Defines when this state should time out and for which reason */
      def timeout: Option[Timeout]
    }
    object State {
      /** A newly reserved resident task */
      case class New(timeout: Option[Timeout]) extends State
      /** A launched resident task, never has a timeout */
      case object Launched extends State {
        override def timeout: Option[Timeout] = None
      }
      /** A resident task that has been running before but terminated and can be relaunched */
      case class Suspended(timeout: Option[Timeout]) extends State
      /** A resident task whose reservation and persistent volumes are being destroyed */
      case class Garbage(timeout: Option[Timeout]) extends State
      /** An unknown resident task created because of unknown reservations/persistent volumes */
      case class Unknown(timeout: Option[Timeout]) extends State

      implicit object StateFormat extends Format[State] {
        override def reads(json: JsValue): JsResult[State] = {
          (json \ "timeout").validateOpt[Timeout].flatMap { timeout =>
            (json \ "name").validate[String].map {
              case "new" => New(timeout)
              case "launched" => Launched
              case "suspended" => Suspended(timeout)
              case "garbage" => Garbage(timeout)
              case _ => Unknown(timeout)
            }
          }
        }

        override def writes(o: State): JsValue = {
          val timeout = Json.toJson(o.timeout)
          o match {
            case _: New => JsObject(Seq("name" -> JsString("new"), "timeout" -> timeout))
            case Launched => JsObject(Seq("name" -> JsString("launched"), "timeout" -> timeout))
            case _: Suspended => JsObject(Seq("name" -> JsString("suspended"), "timeout" -> timeout))
            case _: Garbage => JsObject(Seq("name" -> JsString("garbage"), "timeout" -> timeout))
            case _: Unknown => JsObject(Seq("name" -> JsString("unknown"), "timeout" -> timeout))
          }
        }
      }
    }
    implicit val reservationFormat = Json.format[Reservation]
  }

  /**
    * A Reserved task carries the information of reserved resources and persistent volumes
    * and is currently not launched.
    *
    * @param taskId The task Id
    * @param reservation Information about the reserved resources and persistent volumes
    */
  case class Reserved(
      taskId: Task.Id,
      agentInfo: Instance.AgentInfo,
      reservation: Reservation,
      status: Status,
      runSpecVersion: Timestamp) extends Task {

    override def reservationWithVolumes: Option[Reservation] = Some(reservation)

    override def launched: Option[Launched] = None

    override def update(op: TaskUpdateOperation): TaskUpdateEffect = op match {
      case TaskUpdateOperation.LaunchOnReservation(newRunSpecVersion, taskStatus, hostPorts) =>
        val updatedTask = LaunchedOnReservation(
          taskId, agentInfo, newRunSpecVersion, taskStatus, hostPorts, reservation)
        TaskUpdateEffect.Update(updatedTask)

      case update: TaskUpdateOperation.MesosUpdate =>
        TaskUpdateEffect.Failure("Mesos task status updates cannot be applied to reserved tasks")
    }
  }

  object Reserved {
    implicit val reservedFormat = Json.format[Reserved]
  }

  case class LaunchedOnReservation(
      taskId: Task.Id,
      agentInfo: Instance.AgentInfo,
      runSpecVersion: Timestamp,
      status: Status,
      hostPorts: Seq[Int],
      reservation: Reservation) extends Task {

    import LaunchedOnReservation.log

    override def reservationWithVolumes: Option[Reservation] = Some(reservation)

    override def launched: Option[Launched] = Some(Task.Launched(hostPorts))

    private[this] def hasStartedRunning: Boolean = status.startedAt.isDefined

    // TODO(PODS): this is the same def as in LaunchedEphemeral
    override def update(op: TaskUpdateOperation): TaskUpdateEffect = op match {
      // exceptional case: the task is already terminal. Don't transition in this case.
      // This might be because the task terminated (e.g. finished) before Marathon issues a kill Request
      // to Mesos. Mesos will likely send back a TASK_LOST status update, because the task is no longer
      // known in Mesos. We'll never want to transition from one terminal state to another as a terminal
      // state should already be distinct enough.
      // related to https://github.com/mesosphere/marathon/pull/4531
      case op: TaskUpdateOperation if this.isTerminal =>
        log.warn(s"received $op for terminal $taskId, ignoring")
        TaskUpdateEffect.Noop

      // case 1: now running
      case TaskUpdateOperation.MesosUpdate(Condition.Running, mesosStatus, now) if !hasStartedRunning =>
        val updated = copy(
          status = status.copy(
            startedAt = Some(now),
            mesosStatus = Some(mesosStatus),
            condition = Condition.Running))
        TaskUpdateEffect.Update(updated)

      // case 2: terminal
      case TaskUpdateOperation.MesosUpdate(newStatus: Terminal, mesosStatus, _) =>
        val updatedTask = Task.Reserved(
          taskId = taskId,
          agentInfo = agentInfo,
          reservation = reservation.copy(state = Task.Reservation.State.Suspended(timeout = None)),
          status = status.copy(
            mesosStatus = Some(mesosStatus),
            // Note the task needs to transition to Reserved, otherwise the instance will not transition to Reserved
            condition = Condition.Reserved
          ),
          runSpecVersion = runSpecVersion
        )
        TaskUpdateEffect.Update(updatedTask)

      // case 3: health or state updated
      case TaskUpdateOperation.MesosUpdate(newStatus, mesosUpdate, _) =>
        updatedHealthOrState(status.mesosStatus, mesosUpdate).map { newTaskStatus =>
          val updatedTask = copy(status = status.copy(
            mesosStatus = Some(newTaskStatus),
            condition = newStatus))
          TaskUpdateEffect.Update(newState = updatedTask)
        } getOrElse {
          log.debug("Ignoring status update for {}. Status did not change.", taskId)
          TaskUpdateEffect.Noop
        }
    }
  }

  object LaunchedOnReservation {
    private val log = LoggerFactory.getLogger(getClass)
    implicit val launchedOnReservationFormat = Json.format[LaunchedOnReservation]
  }

  /** returns the new status if the health status has been added or changed, or if the state changed */
  private[this] def updatedHealthOrState(
    maybeCurrent: Option[MesosProtos.TaskStatus],
    update: MesosProtos.TaskStatus): Option[MesosProtos.TaskStatus] = {

    maybeCurrent match {
      case Some(current) =>
        val healthy = update.hasHealthy && (!current.hasHealthy || current.getHealthy != update.getHealthy)
        val changed = healthy || current.getState != update.getState
        if (changed) {
          Some(update)
        } else {
          None
        }
      case None => Some(update)
    }
  }

  def reservedTasks(tasks: Seq[Task]): Seq[Task.Reserved] = tasks.collect { case r: Task.Reserved => r }

  def tasksById(tasks: Seq[Task]): Map[Task.Id, Task] = tasks.map(task => task.taskId -> task)(collection.breakOut)

  implicit class TaskStatusComparison(val task: Task) extends AnyVal {
    def isReserved: Boolean = task.status.condition == Condition.Reserved
    def isCreated: Boolean = task.status.condition == Condition.Created
    def isError: Boolean = task.status.condition == Condition.Error
    def isFailed: Boolean = task.status.condition == Condition.Failed
    def isFinished: Boolean = task.status.condition == Condition.Finished
    def isKilled: Boolean = task.status.condition == Condition.Killed
    def isKilling: Boolean = task.status.condition == Condition.Killing
    def isRunning: Boolean = task.status.condition == Condition.Running
    def isStaging: Boolean = task.status.condition == Condition.Staging
    def isStarting: Boolean = task.status.condition == Condition.Starting
    def isUnreachable: Boolean = task.status.condition == Condition.Unreachable
    def isGone: Boolean = task.status.condition == Condition.Gone
    def isUnknown: Boolean = task.status.condition == Condition.Unknown
    def isDropped: Boolean = task.status.condition == Condition.Dropped
    def isTerminal: Boolean = task.status.condition.isTerminal
    def isActive: Boolean = task.status.condition.isActive
  }

  implicit object TaskFormat extends Format[Task] {
    override def reads(json: JsValue): JsResult[Task] = {
      json.validate[LaunchedEphemeral].orElse(json.validate[Reserved]).orElse(json.validate[LaunchedOnReservation])
    }

    override def writes(o: Task): JsValue = o match {
      case f: LaunchedEphemeral => Json.toJson(f)(LaunchedEphemeral.launchedEphemeralFormat)
      case r: Reserved => Json.toJson(r)(Reserved.reservedFormat)
      case lr: LaunchedOnReservation => Json.toJson(lr)(LaunchedOnReservation.launchedOnReservationFormat)
    }
  }
}
