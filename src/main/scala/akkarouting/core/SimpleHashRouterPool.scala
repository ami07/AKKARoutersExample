package akkarouting.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akkarouting.core.SimpleHashRouterPool.{PrintProgress, SetupMsg, UpdateMessage}
import scala.util.hashing.{ MurmurHash3 => MH3 }

object SimpleHashRouterPool{
  def props(routername:String,numRoutees:Int): Props = Props(new SimpleHashRouterPool(routername,numRoutees))
  case class UpdateMessage(tuple : List[String], key:String, ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
}

class SimpleHashRouterPool (routername:String, numRoutees:Int) extends Actor with ActorLogging{

  val routees:List[ActorRef] = List.range(0, numRoutees).map(i => context.system.actorOf(Props[WorkerActor], s"${routername}_WorkerActor_$i"))

  override def receive: Receive = {
    case UpdateMessage(tuple : List[String], key:String, ts:Int) =>{
      //fwd the message to a selected routee
      val selectedRouteeIndex = Math.abs(MH3.stringHash(key, MH3.stringSeed)) % numRoutees
      routees(selectedRouteeIndex) ! WorkerActor.UpdateMessage(tuple,ts)
    }

    case SetupMsg(relationName : String) =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(r => r ! WorkerActor.SetupMsg(relationName))
    }

    case PrintProgress =>{
      //fwd the message to all the routees (broadcast)
      routees.foreach(r => r ! WorkerActor.PrintProgress)
    }
  }
}