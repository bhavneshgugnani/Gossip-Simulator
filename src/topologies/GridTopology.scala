package topologies

import akka.actor.ActorRef
import akka.actor.ActorSystem
import traits.Topology
import akka.actor.Props
import actors.GridActor
import messages.UpdateGridActorNeighbours
import vo.ConvergenceCounter
import scala.util.Random

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