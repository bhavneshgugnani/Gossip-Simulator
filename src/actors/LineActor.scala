package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages.UpdateLineActorNeighbours
import constants.Constants
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import messages.GossipIn
import messages.GossipOut
import vo.ConvergenceCounter
import messages.ShutDown
import messages.PushSumIn
import messages.PushSumOut
import scala.util.Random

class LineActor(random: Random, system: ActorSystem, convergenceCounter: ConvergenceCounter, s: BigDecimal, w: BigDecimal) extends Actor with Constants {

  //#Neighbours
  var prev: ActorRef = null
  var next: ActorRef = null
  //#From last round
  var oldS: BigDecimal = s
  var oldW: BigDecimal = w
  //#For current round 
  var newS: BigDecimal = s
  var newW: BigDecimal = w
  //#Last 3 differences between s/w
  var last3: Array[BigDecimal] = Array(-1, -1, -1)
  //private var count: Int = 0
  //#Flags to keep track of state of actor 
  var isGossiping = false
  var numberOfGossipHeard = 0
  var isPushingSum = false
  var hasReceivedSum = false
  var hasConverged = false
  var startTime: Long = -1

  def receive() = {
    case UpdateLineActorNeighbours(prev, next) =>
      this.prev = prev
      this.next = next
    case GossipIn(startTime) =>
      gossipIn(startTime)
    case GossipOut =>
      gossipOut
    case PushSumIn(s, w, startTime) =>
      pushSumIn(s, w, startTime)
    case PushSumOut =>
      pushSumOut
    case ShutDown =>
      context.stop(self)
    case _ => println("Invalid message")
  }

  //#Accepts gossip from a neighbour.
  private def gossipIn(startTime: Long) = {
    numberOfGossipHeard += 1
    if (!isGossiping) {
      convergenceCounter.actorsNotConvergedCounter += 1
      isGossiping = true
      this.startTime = startTime
      import context.dispatcher
      val wakeUp = context.system.scheduler.schedule(0 milliseconds, RoundInterval milliseconds, self, GossipOut)
    }
    if (numberOfGossipHeard >= MinimumGossipsHeard) {
      //#Handles the buffer dump for println() of convergence time
      print("") 
      hasConverged = true
      convergenceCounter.actorsNotConvergedCounter -= 1
    }
    if (convergenceCounter.actorsNotConvergedCounter == 0) {
      printf("System Converged in time : " + (System.currentTimeMillis() - startTime) + " milli seconds")
      startShutDownWave()
    }
  }

  private def gossipOut() = {
    var sent = false
    while (!sent) {
      var randomNum = random.nextInt()
      if (randomNum%2 == 0) {
        if (prev != null) {
          prev ! GossipIn(startTime)
          sent = true
        } else {
          next ! GossipIn(startTime)
          sent = true
        }
      } else {
        if (next != null) {
          next ! GossipIn(startTime)
          sent = true
        } else {
          prev ! GossipIn(startTime)
          sent = true
        }
      }
    }
  }

  private def pushSumIn(s: BigDecimal, w: BigDecimal, startTime: Long) = {
    hasReceivedSum = true
    newS += s
    newW += w
    if (!isPushingSum) {
      convergenceCounter.actorsNotConvergedCounter += 1
      isPushingSum = true
      this.startTime = startTime
      import context.dispatcher
      val wakeUp = context.system.scheduler.schedule(0 milliseconds, RoundInterval milliseconds, self, PushSumOut)
    }
  }

  private def pushSumOut() = {
    if (!hasConverged) {
      if (!hasReceivedSum) {
        last3(0) = last3(1)
        last3(1) = last3(2)
        last3(2) = 0
      } else {
        val outS = newS / 2
        val outW = newW / 2
        newS -= outS
        newW -= outW

        var sent = false
        //#Send half to a random target
        while (!sent) {
          var randomNum = random.nextInt()
          if (randomNum%2 == 0) {
            if (prev != null) {
              prev ! PushSumIn(outS, outW, startTime)
              sent = true
            } else {
              next ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else {
            if (next != null) {
              next ! PushSumIn(outS, outW, startTime)
              sent = true
            } else {
              prev ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          }
        }

        //#Update last 3 transactions 
        last3(0) = last3(1)
        last3(1) = last3(2)
        last3(2) = (oldS / oldW) - (newS / newW)
        oldS = newS
        oldW = newW
      }

      if (last3(0) <= PushSumError && last3(1) <= PushSumError && last3(2) <= PushSumError && last3(0) != -1) {
        convergenceCounter.actorsNotConvergedCounter -= 1
        hasConverged = true
      }
      if (convergenceCounter.actorsNotConvergedCounter == 0) {
        val endTime: Long = System.currentTimeMillis()
        println("Convergence Time for Push-Sum is : " + (endTime - startTime) + "milli seconds")
        startShutDownWave
      }
    }
  }

  def startShutDownWave() = {
    context.system.shutdown
    /*prev ! ShutDown
    next ! ShutDown
    self ! ShutDown*/
  }
}