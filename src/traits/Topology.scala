package traits

import akka.actor.ActorRef
import vo.ConvergenceCounter

trait Topology {

  def createTopology(numNodes: Int, convergenceCounter: ConvergenceCounter): ActorRef

}