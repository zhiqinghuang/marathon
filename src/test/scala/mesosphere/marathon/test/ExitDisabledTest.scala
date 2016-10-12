package mesosphere.marathon.test

import java.security.Permission

import akka.actor.{ ActorSystem, Scheduler }
import mesosphere.marathon.util.{ Lock, Retry }
import org.scalatest.Suite

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * Mixin that will disable System.exit while the suite is running.
  */
trait ExitDisabledTest extends Suite {

  def exitCalled(desiredCode: Int)(implicit system: ActorSystem, scheduler: Scheduler): Future[Boolean] = {
    implicit val ctx = system.dispatcher
    Retry.blocking("Check for exit", Int.MaxValue, 1.micro, 5.seconds) {
      if (ExitDisabledTest.exitsCalled(_.contains(desiredCode))) {
        ExitDisabledTest.exitsCalled(e => e.remove(e.indexOf(desiredCode)))
        true
      } else {
        throw new Exception("Did not find desired exit code.")
      }
    }.recover {
      case NonFatal(_) => false
    }
  }
}

object ExitDisabledTest {
  private val exitsCalled = Lock(mutable.ArrayBuffer.empty[Int])
  System.setSecurityManager(new ExitDisabledSecurityManager)

  private class ExitDisabledSecurityManager() extends SecurityManager {
    override def checkExit(i: Int): Unit = {
      exitsCalled(_ :+ i)
      throw new IllegalStateException(s"Attempted to call exit with code: $i")
    }

    override def checkPermission(permission: Permission): Unit = {
      if ("exitVM".equals(permission.getName)) {
        throw new IllegalStateException("Attempted to call exitVM")
      }
    }

    override def checkPermission(permission: Permission, o: scala.Any): Unit = {
      if ("exitVM".equals(permission.getName)) {
        throw new IllegalStateException("Attempted to call exitVM")
      }
    }
  }
}
