package messages

import akka.actor.ActorRef

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