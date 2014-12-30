package proj2

import akka.actor.ActorSystem
import traits.Topology
import topologies.LineTopology
import akka.actor.ActorRef
import topologies.GridTopology
import topologies.FullNetworkTopology
import topologies.ImperfectGridTopology
import messages.GossipIn
import vo.ConvergenceCounter
import vo.ConvergenceCounter
import messages.PushSumIn
import scala.util.Random

object Main {

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
      println("Gossip Started.")
      startTime = System.currentTimeMillis()
      ref ! GossipIn(startTime)
    } else if (algorithm == "push-sum") {
      println("Push-Sum started")
      startTime = System.currentTimeMillis()
      ref ! PushSumIn(0, 0, startTime)
    }

  }
}