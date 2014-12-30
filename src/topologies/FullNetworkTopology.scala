package topologies

import akka.actor.ActorSystem
import traits.Topology
import akka.actor.ActorRef
import akka.actor.Props
import actors.FullNetworkActor
import messages.UpdateFullNetworkActorNeighbours
import vo.ConvergenceCounter
import scala.util.Random

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