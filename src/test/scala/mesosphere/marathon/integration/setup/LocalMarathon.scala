package mesosphere.marathon.integration.setup

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.MarathonApp
import mesosphere.marathon.core.health.HealthCheck
import mesosphere.marathon.integration.facades.{MarathonFacade, MesosFacade}
import mesosphere.marathon.state.{AppDefinition, PathId}
import mesosphere.marathon.util.Retry
import mesosphere.util.PortAllocator
import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

/**
  * Runs marathon _in process_ with all of the dependencies wired up.
  */
case class LocalMarathon(
    autoStart: Boolean = true,
    conf: Map[String, String] = Map.empty) extends AutoCloseable {
  val zk = ZookeeperServer()
  val mesos = MesosLocal()
  private val semaphore = new Semaphore(0)
  private var started = false
  private[setup] implicit lazy val system = ActorSystem()
  private implicit lazy val materializer = ActorMaterializer()
  private implicit lazy val scheduler = system.scheduler
  private implicit lazy val ctx = ExecutionContext.global

  lazy val httpPort = PortAllocator.ephemeralPort()
  private val workDir = {
    val f = Files.createTempDirectory(s"marathon-$httpPort").toFile
    f.deleteOnExit()
    f
  }
  private def write(dir: File, fileName: String, content: String): String = {
    val file = File.createTempFile(fileName, "", dir)
    file.deleteOnExit()
    FileUtils.write(file, content)
    file.setReadable(true)
    file.getAbsolutePath
  }

  private val secretPath = write(workDir, fileName = "marathon-secret", content = "secret1")

  lazy val config = conf ++ Map(
    "master" -> s"zk://${zk.connectUri}/marathon",
    "mesos_authentication_principal" -> "principal",
    "mesos_role" -> "foo",
    "http_port" -> httpPort.toString,
    "zk" -> s"zk://${zk.connectUri}/marathon",
    "mesos_authentication_secret_file" -> s"$secretPath"
  )

  private var closing = false
  private val marathon = new MarathonApp(config.flatMap { case (k, v) => Seq(s"--$k", v) }(collection.breakOut))

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      while (!closing) {
        marathon.start()
        semaphore.acquire()
      }
    }
  }, s"marathon-$httpPort")

  if (autoStart) {
    start()
  }

  def start(): Unit = {
    if (!started) {
      if (thread.getState == Thread.State.NEW) {
        thread.start()
      }
      started = true
      semaphore.release()
      Retry(s"marathon-$httpPort") {
        Http(system).singleRequest(Get(s"http://localhost:$httpPort/version"))
      }
    }
  }

  def stop(): Unit = if (started) {
    marathon.shutdownAndWait()
    started = false
  }

  override def close(): Unit = {
    closing = true
    Try(zk.close())
    Try(mesos.close())
    Try(stop())
    thread.interrupt()
    thread.join()
    Await.result(system.terminate(), 5.seconds)
  }
}

trait LocalMarathonTest extends BeforeAndAfterAll { this: Suite with StrictLogging with ScalaFutures =>
  val marathonServer = LocalMarathon(autoStart = true)
  private implicit val actorSystem = marathonServer.system
  private implicit val mat = ActorMaterializer

  val testBasePath: PathId = PathId(s"/$suiteName")
  lazy val marathon =
    new MarathonFacade(s"http://localhost:${marathonServer.httpPort}/", testBasePath)
  lazy val mesos = new MesosFacade(s"http://localhost:${marathonServer.mesos.port}")

  val events = new ConcurrentLinkedQueue[CallbackEvent]()

  val callbackEndpoint = {
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.server.Directives._

    val route = path("/") {
      (post & entity(as[Map[String, String]])) { event =>
        val kind = event.get("eventType").map(_.toString).getOrElse("unknown")
        logger.info(s"Recieved callback event: $kind with props $event")
        events.add(CallbackEvent(kind, event))
        complete("ok")
      }
    }
    val port = PortAllocator.ephemeralPort()
    val server = Http().bindAndHandle(route, "localhost", port).futureValue
    marathon.subscribe(s"http://localhost:$port")
    server
  }


  abstract override def afterAll(): Unit = {
    marathon.unsubscribe(s"http://localhost:${callbackEndpoint.localAddress.getPort}")
    callbackEndpoint.unbind().futureValue
    Try(marathonServer.close())
    super.afterAll()
  }

  def appProxy(appId: PathId, versionId: String, instances: Int,
    withHealth: Boolean = true, dependencies: Set[PathId] = Set.empty): AppDefinition = {
    val appProxyMainInvocationImpl: String = {
      val javaExecutable = sys.props.get("java.home").fold("java")(_ + "/bin/java")
      val classPath = sys.props.getOrElse("java.class.path", "target/classes").replaceAll(" ", "")
      val main = classOf[AppMock].getName
      s"""$javaExecutable -Xmx64m -classpath $classPath $main"""
    }
    val appProxyMainInvocation: String = {
      val file = File.createTempFile("appProxy", ".sh")
      file.deleteOnExit()

      FileUtils.write(
        file,
        s"""#!/bin/sh
            |set -x
            |exec $appProxyMainInvocationImpl $$*""".stripMargin)
      file.setExecutable(true)

      file.getAbsolutePath
    }
    val cmd = Some(s"""echo APP PROXY $$MESOS_TASK_ID RUNNING; $appProxyMainInvocation """ +
      s"""$appId $versionId http://localhost:${marathonServer.httpPort}/health$appId/$versionId""")

    lazy val appProxyHealthChecks = Set(
      HealthCheck(gracePeriod = 20.second, interval = 1.second, maxConsecutiveFailures = 10))

    AppDefinition(
      id = appId,
      cmd = cmd,
      executor = "//cmd",
      instances = instances,
      cpus = 0.5,
      mem = 128.0,
      healthChecks = if (withHealth) appProxyHealthChecks else Set.empty[HealthCheck],
      dependencies = dependencies
    )
  }

  def cleanUp(withSubscribers: Boolean = false, maxWait: FiniteDuration = 30.seconds) {
    logger.info("Starting to CLEAN UP !!!!!!!!!!")

    WaitTestSupport.waitUntil("Deleted Group", 45.seconds) {
      marathon.deleteGroup(testBasePath, force = true).code != 404
    }

    require(mesos.state.value.agents.size == 1, "one agent expected")
    WaitTestSupport.waitUntil("clean slate in Mesos", 45.seconds) {
      val agent = mesos.state.value.agents.head
      val empty = agent.usedResources.isEmpty && agent.reservedResourcesByRole.isEmpty
      if (!empty) {
        import mesosphere.marathon.integration.facades.MesosFormats._
        logger.info(
          "Waiting for blank slate Mesos...\n \"used_resources\": "
            + Json.prettyPrint(Json.toJson(agent.usedResources)) + "\n \"reserved_resources\": "
            + Json.prettyPrint(Json.toJson(agent.reservedResourcesByRole))
        )
      }
      empty
    }

    val apps = marathon.listAppsInBaseGroup
    require(apps.value.isEmpty, s"apps weren't empty: ${apps.entityPrettyJsonString}")
    val groups = marathon.listGroupsInBaseGroup
    require(groups.value.isEmpty, s"groups weren't empty: ${groups.entityPrettyJsonString}")

    logger.info("CLEAN UP finished !!!!!!!!!")
  }
}
