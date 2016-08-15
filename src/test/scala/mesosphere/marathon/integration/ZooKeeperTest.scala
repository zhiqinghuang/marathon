package mesosphere.marathon.integration

import java.util

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.integration.setup._
import org.apache.zookeeper.ZooDefs.Perms
import org.apache.zookeeper.data.{ ACL, Id }
import org.apache.zookeeper.{ WatchedEvent, Watcher, ZooDefs, ZooKeeper }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.concurrent.duration._

class ZooKeeperTest extends IntegrationFunSuite
    with EmbeddedMarathonTest
    with GivenWhenThen with Matchers
    with ScalaFutures
    with StrictLogging {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("/marathon has OPEN_ACL_UNSAFE acls") {
    Given("a leader has been elected")
    val watcher = new Watcher { override def process(event: WatchedEvent): Unit = {} }
    val zooKeeper = new ZooKeeper(zkServer.connectUri, 30 * 1000, watcher)

    try {
      Then("the /leader node exists")
      val stat = zooKeeper.exists("/marathon/leader", false)
      Option(stat) should not be empty

      And("it has the default OPEN_ACL_UNSAFE permissions")
      val acls = zooKeeper.getACL("/marathon/leader", stat)
      val expectedAcl = new util.ArrayList[ACL]
      expectedAcl.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      expectedAcl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
      acls.toArray.toSet should equal(expectedAcl.toArray.toSet)
    } finally {
      zooKeeper.close()
    }
  }
}

class AuthorizedZooKeeperTest extends IntegrationFunSuite
    with EmbeddedMarathonTest with GivenWhenThen with Matchers with ScalaFutures
    with StrictLogging {

  lazy val credentials = "user:secret"
  lazy val digest = org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest(credentials)

  override val marathonArgs = Map("zk" -> s"zk://$credentials@${zkServer.connectUri}/marathon")

  test("/marathon has OPEN_ACL_UNSAFE acls") {
    val watcher = new Watcher { override def process(event: WatchedEvent): Unit = {} }
    val zooKeeper = new ZooKeeper(zkServer.connectUri, 30 * 1000, watcher)
    zooKeeper.addAuthInfo("digest", digest.getBytes("UTF-8"))

    try {
      Given("a leader has been elected")
      WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) {
        marathon.leader().code == 200
      }

      Then("the /leader node exists")
      var stat = zooKeeper.exists("/marathon/leader", false)
      Option(stat) should not be empty

      And(s"the /leader node has $credentials:rcdwa + world:r")
      var acls = zooKeeper.getACL("/marathon/leader", stat)
      var expectedAcl = new util.ArrayList[ACL]
      expectedAcl.add(new ACL(Perms.ALL, new Id("digest", digest)))
      expectedAcl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
      acls.toArray.toSet should equal(expectedAcl.toArray.toSet)

      Then("the /state node exists")
      stat = zooKeeper.exists("/marathon/state", false)
      Option(stat) should not be empty

      And(s"the /state node has $credentials:rcdwa")
      acls = zooKeeper.getACL("/marathon/state", stat)
      expectedAcl = new util.ArrayList[ACL]
      expectedAcl.add(new ACL(Perms.ALL, new Id("digest", digest)))
      acls.toArray.toSet should equal(expectedAcl.toArray.toSet)
    } finally {
      zooKeeper.close()
    }
  }
}