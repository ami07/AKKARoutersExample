package akkarouting.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akkarouting.core.SimpleHashRouter.{PrintProgress, SetupMsg, UpdateMessage}


object SimpleHashRouter{
  def props(routees:List[ActorRef]): Props = Props(new SimpleHashRouter(routees))
  case class UpdateMessage(tuple : List[String], key:Int, ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
}

class SimpleHashRouter(routees:List[ActorRef]) extends Actor with ActorLogging{

  val numRoutees = routees.length

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
