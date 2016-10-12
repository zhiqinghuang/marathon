package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationFunTest
import mesosphere.marathon.core.base.RichRuntime
import mesosphere.marathon.integration.facades.MarathonFacade
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.state.PathId
import mesosphere.marathon.util.Retry
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ WatchedEvent, Watcher, ZooKeeper }
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration._
import scala.util.Try

@IntegrationTest
class LeaderIntegrationTest extends AkkaIntegrationFunTest with MarathonClusterTest {

  test("all nodes return the same leader") {
    Given("a leader has been elected")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }

    When("calling /v2/leader on all nodes of a cluster")
    val results = marathonFacades.map(marathon => marathon.leader())

    Then("the requests should all be successful")
    results.foreach(_.code should be (200))

    And("they should all be the same")
    results.map(_.value).distinct should have length 1
  }

  test("all nodes return a redirect on GET /") {
    Given("a leader has been elected")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }

    When("get / on all nodes of a cluster")
    val results = marathonFacades.map(marathon => marathon.getPath("/"))

    Then("all nodes send a redirect")
    results.foreach(_.code should be (302))
  }

  test("the leader abdicates when it receives a DELETE") {
    Given("a leader")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }
    val leader = marathon.leader().value

    When("calling DELETE /v2/leader")
    val result = marathon.abdicate()

    Then("the request should be successful")
    result.code should be (200)
    (result.entityJson \ "message").as[String] should be ("Leadership abdicated")

    And("the leader must have changed")
    WaitTestSupport.waitUntil("the leader changes", 30.seconds) { marathon.leader().value != leader }
  }

  ignore("it survives a small burn-in reelection test - https://github.com/mesosphere/marathon/issues/4215") {
    val random = new scala.util.Random
    for (_ <- 1 to 10) {
      Given("a leader")
      WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }
      val leader = marathon.leader().value

      When("calling DELETE /v2/leader")
      val result = marathon.abdicate()

      Then("the request should be successful")
      result.code should be (200)
      (result.entityJson \ "message").as[String] should be ("Leadership abdicated")

      And("the leader must have changed")
      WaitTestSupport.waitUntil("the leader changes", 30.seconds) {
        val result = marathon.leader()
        result.code == 200 && result.value != leader
      }

      And("all instances agree on the leader")
      WaitTestSupport.waitUntil("all instances agree on the leader", 30.seconds) {
        val results = marathonFacades.map(marathon => marathon.leader())
        results.forall(_.code == 200) && results.map(_.value).distinct.size == 1
      }
    }
  }

  test("the leader sets a tombstone for the old twitter commons leader election") {
    def checkTombstone(): Unit = {
      val watcher = new Watcher { override def process(event: WatchedEvent): Unit = println(event) }
      val zooKeeper = new ZooKeeper(zkServer.connectUri, 30 * 1000, watcher)

      try {
        Then("there is a tombstone")
        var stat: Option[Stat] = None
        WaitTestSupport.waitUntil("the tombstone is created", 30.seconds) {
          stat = Option(zooKeeper.exists("/marathon/leader/member_-00000000", false))
          stat.isDefined
        }

        And("the tombstone points to the leader")
        val apiLeader: String = marathon.leader().value.leader
        val tombstoneData = zooKeeper.getData("/marathon/leader/member_-00000000", false, stat.get)
        new String(tombstoneData, "UTF-8") should equal(apiLeader)
      } finally {
        zooKeeper.close()
      }
    }

    Given("a leader")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }
    val leader = marathon.leader().value

    checkTombstone()

    When("calling DELETE /v2/leader")
    val result = marathon.abdicate()

    Then("the request should be successful")
    result.code should be (200)
    (result.entityJson \ "message").as[String] should be ("Leadership abdicated")

    And("the leader must have changed")
    WaitTestSupport.waitUntil("the leader changes", 30.seconds) { marathon.leader().value != leader }

    checkTombstone()
  }

  // TODO(jasongilanfarr) Marathon will kill itself in this test so this doesn't actually work and needs to be revisited.
  ignore("the tombstone stops old instances from becoming leader") {
    When("Starting an instance with --leader_election_backend")

    val oldMarathon = LocalMarathon(false, suite = suiteName, marathonServer.masterUrl, marathonServer.config("zk"),
      Map("leader_election_backend" -> "twitter_commons"))
    try {
      oldMarathon.start()
      val facade = new MarathonFacade(s"http://localhost:${oldMarathon.httpPort}", PathId.empty)
      val random = new scala.util.Random

      Retry.blocking("Wait for marathon to start", maxAttempts = 60)(facade.info).futureValue(Timeout(10.seconds))

      1.to(10).map { i =>
        Given(s"a leader ($i)")
        WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) {
          marathon.leader().code == 200
        }
        val leader = marathon.leader().value

        Then(s"it is never the twitter_commons instance ($i)")
        leader.leader.split(":")(1).toInt should not be oldMarathon.httpPort

        And(s"the twitter_commons instance knows the real leader ($i)")
        WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) {
          val result = facade.leader()
          result.code == 200 && result.value == leader
        }

        When(s"calling DELETE /v2/leader ($i)")
        val result = marathon.abdicate()

        Then(s"the request should be successful ($i)")
        result.code should be(200)
        (result.entityJson \ "message").as[String] should be("Leadership abdicated")

        And(s"the leader must have changed ($i)")
        WaitTestSupport.waitUntil("the leader changes", 30.seconds) {
          val result = marathon.leader()
          result.code == 200 && result.value != leader
        }

        Thread.sleep(random.nextInt(10) * 100L)
      }
    } finally {
      oldMarathon.close()
    }
  }

  ignore("commit suicide if the zk connection is dropped") {
    // FIXME (gkleiman): investigate why this test fails (https://github.com/mesosphere/marathon/issues/3566)
    Given("a leader")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) { marathon.leader().code == 200 }

    When("ZooKeeper dies")
    zkServer.stop()

    Then("Marathon commits suicide")
    exitCalled(RichRuntime.FatalErrorSignal).futureValue should be(true)

    When("Zookeeper starts again")
    zkServer.start()
    marathonServer.start()

    Then("A new leader is elected")
    WaitTestSupport.waitUntil("a leader has been elected", 30.seconds) {
      Try(marathon.leader().code).getOrElse(500) == 200
    }
  }
}
