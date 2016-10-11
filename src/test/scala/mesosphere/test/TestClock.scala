package mesosphere.test

import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.state.Timestamp

/**
  * Mixin that provides an internal clock that produces unique timestamps.
  */
trait Clocked {
  /**
    * Retrieve a unique timestamp
    * the clock will be altered for this so that the same timestamp is never retrieved twice
    * @return
    */
  def tick(): Timestamp = TestClock.tick()

  /**
    * Do not use this reference to retrieve timestamps in tests!
    * Should be used only for wiring components that need a clock.
    */
  def clock = TestClock.clock
}

private[test] object TestClock {
  private[test] val clock = ConstantClock()

  def tick(): Timestamp = {
    import scala.concurrent.duration._
    clock += 1.second
    clock.now()
  }

}
