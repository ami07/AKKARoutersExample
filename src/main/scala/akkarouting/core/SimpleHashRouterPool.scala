package akkarouting.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akkarouting.core.SimpleHashRouterPool.{PrintProgress, SetupMsg, UpdateMessage}

object SimpleHashRouterPool{
  def props(numRoutees:Int): Props = Props(new SimpleHashRouterPool(numRoutees))
  case class UpdateMessage(tuple : List[String], key:Int, ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
}

class SimpleHashRouterPool (numRoutees:Int) extends Actor with ActorLogging{

  val routees:List[ActorRef] = List.range(0, 10).map(i => context.system.actorOf(Props[WorkerActor], s"WorkerActor_$i"))

  override def receive: Receive = {
    case UpdateMessage(tuple : List[String], key:Int, ts:Int) =>{
      //fwd the message to a selected routee
      val selectedRouteeIndex = key % numRoutees
      routees(selectedRouteeIndex) ! WorkerActor.UpdateMessage(tuple,ts)
    }

    case SetupMsg(relationName : String) =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(_ ! WorkerActor.SetupMsg(relationName))
    }

    case PrintProgress =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(_ ! WorkerActor.PrintProgress)
    }
  }
}