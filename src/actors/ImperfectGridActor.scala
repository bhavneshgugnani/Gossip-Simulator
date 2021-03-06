package actors

import akka.actor.Actor
import constants.Constants
import akka.actor.ActorRef
import messages.UpdateImperfectGridActorNeighbours
import messages.ShutDown
import akka.actor.ActorSystem
import vo.ConvergenceCounter
import messages.UpdateLineActorNeighbours
import messages.GossipIn
import messages.GossipOut
import messages.PushSumIn
import messages.PushSumOut
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.util.Random

class ImperfectGridActor(rand: Random, system: ActorSystem, convergenceCounter: ConvergenceCounter, s: BigDecimal, w: BigDecimal) extends Actor with Constants {

  //#Neighbours
  var top: ActorRef = null
  var left: ActorRef = null
  var right: ActorRef = null
  var bottom: ActorRef = null
  var random: ActorRef = null
  //#From last round
  var oldS: BigDecimal = s
  var oldW: BigDecimal = w
  //#For current round 
  var newS: BigDecimal = s
  var newW: BigDecimal = w
  //#Last 3 differences between s/w
  var last3: Array[BigDecimal] = Array(-1, -1, -1)
  //#Flags to keep track of state of actor
  var numberOfGossipHeard = 0
  var isGossiping = false
  var isPushingSum = false
  var hasReceivedSum = false
  var hasConverged = false
  var startTime: Long = -1

  def receive() = {
    case UpdateImperfectGridActorNeighbours(top, left, right, bottom, random) =>
      this.top = top
      this.left = left
      this.right = right
      this.bottom = bottom
      this.random = random
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
      val randValue = rand.nextInt(5)
      if (randValue == 0) {
        if (top != null) {
          top ! GossipIn(startTime)
          sent = true
        }
      } else if (randValue == 1) {
        if (left != null) {
          left ! GossipIn(startTime)
          sent = true
        }
      } else if (randValue == 2) {
        if (right != null) {
          right ! GossipIn(startTime)
          sent = true
        }
      } else if (randValue == 3) {
        if (bottom != null) {
          bottom ! GossipIn(startTime)
          sent = true
        }
      } else if (randValue == 4) {
        if (random != null) {
          random ! GossipIn(startTime)
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
          val randValue = rand.nextInt()
          if (randValue % 5 == 0) {
            if (top != null) {
              top ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue % 5 == 1) {
            if (left != null) {
              left ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue % 5 == 2) {
            if (right != null) {
              right ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue % 5 == 3) {
            if (bottom != null) {
              bottom ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue % 5 == 4) {
            if (random != null) {
              random ! PushSumIn(outS, outW, startTime)
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
    /*top ! ShutDown
    left ! ShutDown
    right ! ShutDown
    bottom ! ShutDown
    random ! ShutDown
    self ! ShutDown*/
  }

}