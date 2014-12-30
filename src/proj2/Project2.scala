import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.util.Random
import akka.actor.Props
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import akka.actor.Actor

object Project2 {

  def main(args: Array[String]) {
    //#Arguments
    val numNodes = args(0).toInt
    val topology = args(1)
    val algorithm = args(2)
    //#Actor system
    val system = ActorSystem("Project2")
    val random = new Random
    var projTopology: Topology = null

    if (topology == "line") {
      projTopology = new LineTopology(random, system, numNodes)
    } else if (topology == "2D") {
      projTopology = new GridTopology(random, system, numNodes)
    } else if (topology == "full") {
      projTopology = new FullNetworkTopology(random, system, numNodes)
    } else if (topology == "imp2D") {
      projTopology = new ImperfectGridTopology(random, system, numNodes)
    }
    //#Create topology
    var convergenceCounter: ConvergenceCounter = new ConvergenceCounter
    val ref: ActorRef = projTopology.createTopology(numNodes, convergenceCounter)
    var startTime: Long = -1
    //#Start algorithm execution
    if (algorithm == "gossip") {
      setNumberOfNodes(topology, convergenceCounter, numNodes)
      println("Gossip Started.")
      startTime = System.currentTimeMillis()
      ref ! GossipIn(startTime)
    } else if (algorithm == "push-sum") {
      println("Push-Sum started")
      startTime = System.currentTimeMillis()
      ref ! PushSumIn(0, 0, startTime)
    }
  }

  private def setNumberOfNodes(topology: String, convergenceCounter: ConvergenceCounter, numNodes: Int) = {
    if (topology == "line" || topology == "full")
      convergenceCounter.actorsNotConvergedCounter = numNodes
    else
      convergenceCounter.actorsNotConvergedCounter = Math.pow(Math.ceil(Math.sqrt(numNodes)).toDouble, 2).toInt
  }
}

class LineTopology(random: Random, system: ActorSystem, numNodes: Int) extends Topology {

  override def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef = {
    //convergenceCounter.actorsNotConvergedCounter = numNodes
    val actorRefArray: Array[ActorRef] = new Array[ActorRef](numNodes)
    //#Generate actors
    for (i <- 1 to numNodes) {
      actorRefArray(i - 1) = system.actorOf(Props(new LineActor(random, system, convergenceCounter, i, 1)), "LineActor" + i)
    }
    //#Assign neighbours to each actor
    actorRefArray(0) ! UpdateLineActorNeighbours(null, actorRefArray(1))
    for (i <- 1 to numNodes - 2) {
      actorRefArray(i) ! UpdateLineActorNeighbours(actorRefArray(i - 1), actorRefArray(i + 1))
    }
    actorRefArray(numNodes - 1) ! UpdateLineActorNeighbours(actorRefArray(numNodes - 2), null)
    //#Return reference of node for starting gossip/push-sum
    actorRefArray(0)
  }
}

class ImperfectGridTopology(random: Random, system: ActorSystem, numNodes: Int) extends Topology {

  override def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef = {
    val gridDimension = Math.ceil(Math.sqrt(numNodes)).toInt
    //convergenceCounter.actorsNotConvergedCounter = (gridDimension * gridDimension)
    val actorRefGrid: Array[Array[ActorRef]] = new Array[Array[ActorRef]](gridDimension)
    var counter = 1
    //#Generate actors
    for (i <- 0 to gridDimension - 1) {
      actorRefGrid(i) = new Array[ActorRef](gridDimension)
      for (j <- 0 to gridDimension - 1) {
        actorRefGrid(i)(j) = system.actorOf(Props(new ImperfectGridActor(random, system, convergenceCounter, counter, 1)), "ImperfectGridActor" + counter)
        counter += 1
      }
    }
    //#Assign neighbours to each actor
    var top: ActorRef = null
    var left: ActorRef = null
    var right: ActorRef = null
    var bottom: ActorRef = null
    for (i <- 0 to gridDimension - 1) {
      for (j <- 0 to gridDimension - 1) {
        if (i == 0)
          top = null
        else
          top = actorRefGrid(i - 1)(j)
        if (j == 0)
          left = null
        else
          left = actorRefGrid(i)(j - 1)
        if (j == gridDimension - 1)
          right = null
        else
          right = actorRefGrid(i)(j + 1)
        if (i == gridDimension - 1)
          bottom = null
        else
          bottom = actorRefGrid(i + 1)(j)
        var randomActorRef: ActorRef = actorRefGrid(random.nextInt(gridDimension))(random.nextInt(gridDimension))
        actorRefGrid(i)(j) ! UpdateImperfectGridActorNeighbours(top, left, right, bottom, randomActorRef)
      }
    }
    //#Return reference of node for starting gossip/push-sum
    actorRefGrid(0)(0)
  }
}

class GridTopology(random: Random, system: ActorSystem, numNodes: Int) extends Topology {

  override def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef = {
    val gridDimension = Math.ceil(Math.sqrt(numNodes)).toInt
    //convergenceCounter.actorsNotConvergedCounter = (gridDimension * gridDimension)
    val actorRefGrid: Array[Array[ActorRef]] = new Array[Array[ActorRef]](gridDimension)
    var counter = 0
    //#Generate actors
    for (i <- 0 to gridDimension - 1) {
      actorRefGrid(i) = new Array[ActorRef](gridDimension)
      for (j <- 0 to gridDimension - 1) {
        actorRefGrid(i)(j) = system.actorOf(Props(new GridActor(random, system, convergenceCounter, counter, 1)), "GridActor" + (counter))
        counter += 1
      }
    }
    //#Assign neighbours to each actor
    var top: ActorRef = null
    var left: ActorRef = null
    var right: ActorRef = null
    var bottom: ActorRef = null
    for (i <- 0 to gridDimension - 1) {
      for (j <- 0 to gridDimension - 1) {
        if (i == 0) // || i > gridDimension-1 || j < 0 || j > gridDimension-1)
          top = null
        else
          top = actorRefGrid(i - 1)(j)
        if (j == 0)
          left = null
        else
          left = actorRefGrid(i)(j - 1)
        if (j == gridDimension - 1)
          right = null
        else
          right = actorRefGrid(i)(j + 1)
        if (i == gridDimension - 1)
          bottom = null
        else
          bottom = actorRefGrid(i + 1)(j)
        actorRefGrid(i)(j) ! UpdateGridActorNeighbours(top, left, right, bottom)
      }
    }
    //#Return reference of node for starting gossip/push-sum
    actorRefGrid(0)(0)
  }
}

class FullNetworkTopology(random: Random, system: ActorSystem, numNodes: Int) extends Topology {

  override def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef = {
    //convergenceCounter.actorsNotConvergedCounter = numNodes
    val actorRefArray: Array[ActorRef] = new Array[ActorRef](numNodes)
    //#Generate actors
    for (i <- 1 to numNodes) {
      actorRefArray(i - 1) = system.actorOf(Props(new FullNetworkActor(random, system, convergenceCounter, i, 1)), "FullNetworkActor" + i)
    }
    //#Assign neighbours to each actor
    for (i <- 0 to numNodes - 1) {
      actorRefArray(i) ! UpdateFullNetworkActorNeighbours(actorRefArray, i)
    }
    //#Return reference of node for starting gossip/push-sum
    actorRefArray(0)
  }
}

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
      //convergenceCounter.actorsNotConvergedCounter += 1
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
      if (randomNum % 2 == 0) {
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
          if (randomNum % 2 == 0) {
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
      //convergenceCounter.actorsNotConvergedCounter += 1
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

class GridActor(rand: Random, system: ActorSystem, convergenceCounter: ConvergenceCounter, s: BigDecimal, w: BigDecimal) extends Actor with Constants {

  //#Neighbours
  var top: ActorRef = null
  var left: ActorRef = null
  var right: ActorRef = null
  var bottom: ActorRef = null
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
    case UpdateGridActorNeighbours(top, left, right, bottom) =>
      this.top = top
      this.left = left
      this.right = right
      this.bottom = bottom
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
      //convergenceCounter.actorsNotConvergedCounter += 1
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
      val randValue = rand.nextInt(4)
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
          val randValue = rand.nextInt(4)
          if (randValue == 0) {
            if (top != null) {
              top ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue == 1) {
            if (left != null) {
              left ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue == 2) {
            if (right != null) {
              right ! PushSumIn(outS, outW, startTime)
              sent = true
            }
          } else if (randValue == 3) {
            if (bottom != null) {
              bottom ! PushSumIn(outS, outW, startTime)
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
    self ! ShutDown*/
  }

}

class FullNetworkActor(rand: Random, system: ActorSystem, convergenceCounter: ConvergenceCounter, s: BigDecimal, w: BigDecimal) extends Actor with Constants {

  //#Neighbours
  var allNetworkActor: Array[ActorRef] = null
  var currIndex: Int = -1
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
    case UpdateFullNetworkActorNeighbours(allNetworkActor, currIndex) =>
      this.allNetworkActor = allNetworkActor
      this.currIndex = currIndex
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
      //convergenceCounter.actorsNotConvergedCounter += 1
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
      var randomNum = rand.nextInt(allNetworkActor.length)
      if (randomNum != currIndex && allNetworkActor(randomNum) != null) {
        allNetworkActor(randomNum) ! GossipIn(startTime)
        sent = true
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
          var randomNum = rand.nextInt(allNetworkActor.length)
          if (randomNum != currIndex && allNetworkActor(randomNum) != null) {
            allNetworkActor(randomNum) ! PushSumIn(outS, outW, startTime)
            sent = true
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
    /*for (ref <- allNetworkActor)
      ref ! ShutDown*/
  }

}

trait Topology {

  def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef

}

class ConvergenceCounter {

  @volatile var actorsNotConvergedCounter: Int = -1

}

trait Constants {
  //#Window size of interval
  val RoundInterval = 100
  //#Push sum error for last 3 messages before actor shuts down
  val PushSumError = 0.0000000001
  //#Minimum gossips to be heard by actor before shutdown
  val MinimumGossipsHeard = 1
}

sealed trait Messages
//#Incoming gossip from actor
case class GossipIn(startTime: Long) extends Messages
//#Scheduled Outgoing gossip to random neighbour/actor
case class GossipOut() extends Messages
//#Pushing Sum into actor
case class PushSumIn(s: BigDecimal, w: BigDecimal, startTime: Long) extends Messages
//#Pushing sum out of actor
case class PushSumOut() extends Messages
//#Update neighbours for each actor in network
case class UpdateLineActorNeighbours(prev: ActorRef, next: ActorRef) extends Messages
case class UpdateFullNetworkActorNeighbours(allNetworkActor: Array[ActorRef], currIndex: Int) extends Messages
case class UpdateGridActorNeighbours(top: ActorRef, left: ActorRef, right: ActorRef, bottom: ActorRef) extends Messages
case class UpdateImperfectGridActorNeighbours(top: ActorRef, left: ActorRef, right: ActorRef, bottom: ActorRef, random: ActorRef) extends Messages
//#Shutdown for actor
case class ShutDown() extends Messages