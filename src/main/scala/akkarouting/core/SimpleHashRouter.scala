package akkarouting.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akkarouting.core.SimpleHashRouter._


object SimpleHashRouter{
  def props(routername:String,routees:List[ActorRef]): Props = Props(new SimpleHashRouter(routername,routees))
  case class UpdateMessage(tuple : List[String], key:String, ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
  case class UpdateMessageCF(tuple : List[(List[String],String)], keyIndex:Int, ts:Int)
  case class SetupMsgCF(relationName : String)
  case object PrintProgressCF
}

class SimpleHashRouter(routername:String, routees:List[ActorRef]) extends Actor with ActorLogging{

  val numRoutees = routees.length
  var routedMessges = 0

  override def receive: Receive = {
    case UpdateMessage(tuple : List[String], key:String, ts:Int) =>{
      //fwd the message to a selected routee
      routedMessges +=1
      val selectedRouteeIndex = key.toLong % numRoutees
      routees(selectedRouteeIndex.toInt) ! WorkerActor.UpdateMessage(tuple,ts)
    }

    case SetupMsg(relationName : String) =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(_ ! WorkerActor.SetupMsg(relationName))
    }

    case PrintProgress =>{
      //fwd the message to all the routees (broadcast)
      log.info("Router: total messages "+routedMessges)
      routees.foreach(_ ! WorkerActor.PrintProgress)
    }

    case UpdateMessageCF(tuples : List[(List[String],String)], keyIndex:Int, ts:Int) =>{
      //fwd the message to a selected routee
      routedMessges +=1
      //val selectedRouteeIndex = keyIndex.toLong % numRoutees
      routees(keyIndex) ! WorkerActorCF.UpdateMessageBatch(tuples,ts)
    }

    case SetupMsgCF(relationName : String) =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(_ ! WorkerActorCF.SetupMsg(relationName))
    }

    case PrintProgressCF =>{
      //fwd the message to all the routees (broadcast)
      log.info("Router: total messages "+routedMessges)
      routees.foreach(_ ! WorkerActorCF.PrintProgress)
    }
  }
}
