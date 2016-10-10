package mesosphere.marathon

import com.google.common.util.concurrent.ServiceManager
import com.google.inject.{ Guice, Module }
import mesosphere.chaos.http.{ HttpModule, HttpService }
import mesosphere.chaos.metrics.MetricsModule
import mesosphere.marathon.api.MarathonRestModule
import mesosphere.marathon.core.CoreGuiceModule
import mesosphere.marathon.metrics.{ MetricsReporterModule, MetricsReporterService }
import mesosphere.marathon.storage.StorageModule
import org.slf4j.LoggerFactory
import org.slf4j.bridge.SLF4JBridgeHandler

import scala.collection.JavaConverters._

class MarathonApp(args: Seq[String]) extends AutoCloseable {
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  SLF4JBridgeHandler.install()

  private var running = false
  private val log = LoggerFactory.getLogger(getClass.getName)
  val conf = new AllConf(args)
  protected def modules: Seq[Module] = {
    Seq(
      new HttpModule(conf),
      new MetricsModule,
      new MetricsReporterModule(conf),
      new MarathonModule(conf, conf),
      new MarathonRestModule,
      new DebugModule(conf),
      new CoreGuiceModule
    )
  }
  private var serviceManager: Option[ServiceManager] = None

  def start(): Unit = if (!running) {
    running = true
    setConcurrentContextDefaults()

    log.info(s"Starting Marathon ${BuildInfo.version}/${BuildInfo.buildref} with ${args.mkString(" ")}")

    val injector = Guice.createInjector(modules.asJava)
    val services = Seq(
      classOf[HttpService],
      classOf[MarathonSchedulerService],
      classOf[MetricsReporterService],
      classOf[StorageModule]).map(injector.getInstance(_))
    val serviceManager = new ServiceManager(services.asJava)
    this.serviceManager = Some(serviceManager)

    sys.addShutdownHook(shutdownAndWait())

    serviceManager.startAsync()

    try {
      serviceManager.awaitHealthy()
    } catch {
      case e: Exception =>
        log.error(s"Failed to start all services. Services by state: ${serviceManager.servicesByState()}", e)
        shutdownAndWait()
        throw e
    }

    log.info("All services up and running.")
  }

  def shutdown(): Unit = if (running) {
    running = false
    log.info("Shutting down services")
    serviceManager.foreach(_.stopAsync())
  }

  def shutdownAndWait(): Unit = {
    serviceManager.foreach { serviceManager =>
      shutdown()
      log.info("Waiting for services to shut down")
      serviceManager.awaitStopped()
    }
  }

  override def close(): Unit = shutdownAndWait()

  /**
    * Make sure that we have more than one thread -- otherwise some unmarked blocking operations might cause trouble.
    *
    * See
    * [The Global Execution
    *  Context](http://docs.scala-lang.org/overviews/core/futures.html#the-global-execution-context)
    * in the scala documentation.
    *
    * Here is the relevant excerpt in case the link gets broken:
    *
    * # The Global Execution Context
    *
    * ExecutionContext.global is an ExecutionContext backed by a ForkJoinPool. It should be sufficient for most
    * situations but requires some care. A ForkJoinPool manages a limited amount of threads (the maximum amount of
    * thread being referred to as parallelism level). The number of concurrently blocking computations can exceed the
    * parallelism level only if each blocking call is wrapped inside a blocking call (more on that below). Otherwise,
    * there is a risk that the thread pool in the global execution context is starved, and no computation can proceed.
    *
    * By default the ExecutionContext.global sets the parallelism level of its underlying fork-join pool to the amount
    * of available processors (Runtime.availableProcessors). This configuration can be overriden by setting one
    * (or more) of the following VM attributes:
    *
    * scala.concurrent.context.minThreads - defaults to Runtime.availableProcessors
    * scala.concurrent.context.numThreads - can be a number or a multiplier (N) in the form ‘xN’ ;
    *                                       defaults to Runtime.availableProcessors
    * scala.concurrent.context.maxThreads - defaults to Runtime.availableProcessors
    *
    * The parallelism level will be set to numThreads as long as it remains within [minThreads; maxThreads].
    *
    * As stated above the ForkJoinPool can increase the amount of threads beyond its parallelismLevel in the
    * presence of blocking computation.
    */
  private[this] def setConcurrentContextDefaults(): Unit = {
    def setIfNotDefined(property: String, value: String): Unit = {
      if (!sys.props.contains(property)) {
        sys.props += property -> value
      }
    }

    setIfNotDefined("scala.concurrent.context.minThreads", "5")
    setIfNotDefined("scala.concurrent.context.numThreads", "x2")
    setIfNotDefined("scala.concurrent.context.maxThreads", "64")
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val app = new MarathonApp(args.toVector)
    app.start()
  }
}
