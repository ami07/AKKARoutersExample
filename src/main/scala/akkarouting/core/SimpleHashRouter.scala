package akkarouting.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akkarouting.core.SimpleHashRouter._
import akkarouting.flowcontrol.MasterActor.RequestTuplesR


object SimpleHashRouter{
  def props(routername:String,routees:List[ActorRef]): Props = Props(new SimpleHashRouter(routername,routees))
  case class UpdateMessage(tuple : List[String], key:String, ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
  case class UpdateMessageCF(tuple : List[(List[String],String)], keyIndex:Int, ts:Int)
  case class SetupMsgCF(relationName : String, flowControlThreshold : Int, flowControllerPercentage:Double/*, master : ActorRef*/)
  case object PrintProgressCF
}

class SimpleHashRouter(routername:String, routees:List[ActorRef]) extends Actor with ActorLogging{

  val numRoutees = routees.length
  var routedMessges = 0
  var routedTuples = 0


  var flowController = 0
  var flowControlMessages = 0
  var flowControllerPercentage: Double = 0.0
  var flowControlThreshold = 0


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
      log.debug("Router: received update message for routee with index: "+keyIndex)
      //fwd the message to a selected routee
      routedMessges +=1
      //routedTuples +=tuples.length
      //val selectedRouteeIndex = keyIndex.toLong % numRoutees
      routees(keyIndex) ! WorkerActorCF.UpdateMessageBatchR(tuples,ts)

      //request more tuples from the master
      //TODO possibly have the threshould 75% of the controller value and update the flowControlMessages accordingly
      flowControlMessages +=1
      if(flowControlMessages >= flowControlThreshold /*flowController*/) {
        log.debug("Worker/routee: to pull more tuples from the master")
        sender() ! RequestTuplesR()
        flowControlMessages = flowController - flowControlMessages
      }
    }

    case SetupMsgCF(relationName : String, flowControlVal : Int, flowControllerP: Double/*, master : ActorRef*/) =>{
      flowController = flowControlVal
      flowControllerPercentage = flowControllerP
      flowControlThreshold = (flowController * flowControllerPercentage).toInt

      //fwd the message to all the routees (broadcast)
      routees.foreach(_ ! WorkerActorCF.SetupMsgR(relationName/*, master*/))

      //wait for a little
      Thread.sleep(3000)
      //start pulling from the master node
      sender() ! RequestTuplesR()
      flowControlMessages +=1
    }

    case PrintProgressCF =>{
      //fwd the message to all the routees (broadcast)
      log.info("Router: total messages "+routedMessges+" and total num of tuples "+ routedTuples +" to mark as stream is completed and print progress")
      routees.foreach(_ ! WorkerActorCF.PrintProgress)
    }
  }
}
