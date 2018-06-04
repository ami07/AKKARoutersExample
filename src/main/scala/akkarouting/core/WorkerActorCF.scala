package akkarouting.core

import akka.actor.{Actor, ActorLogging, RootActorPath}
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.MemberUp
import akkarouting.core.WorkerActorCF.{PrintProgress, SetupMsg, UpdateMessage, UpdateMessageBatch}
import akkarouting.flowcontrol.MasterActor.{RequestTuples, WorkerActorRegisteration}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.{HashMap, MultiMap, Set}

object  WorkerActorCF{
  case class UpdateMessage(tuple : List[String], key:String, ts:Int)
  case class UpdateMessageBatch(tuples : List[(List[String],String)], ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
}
class WorkerActorCF extends Actor with ActorLogging{
  import context._

  val view  = new HashMap[String,Set[List[String]]] with  MultiMap[String,List[String]]

  //counters to keep track of time and processed messages
  val startTime = System.nanoTime
  var endTime = System.nanoTime
  var processedMsgs = 0
  var finishedChildren = 0;
  var duplicated = 0


  val config = ConfigFactory.load()

  val isLocalProvider = if(config.getString("akka.actor.provider")=="local") true else false

  val flowController = config.getInt("routingexample.flowController")
  var flowControlMessages = 0
  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = {
    if(!isLocalProvider){
      val cluster = Cluster(context.system)
      cluster.subscribe(self, classOf[MemberUp])
    }
  }
  override def postStop(): Unit = {
    if(!isLocalProvider){
      val cluster = Cluster(context.system)
      cluster.unsubscribe(self)
    }
  }


  def receive = idle
  def idle: Receive = {
    case MemberUp(m) => register(m)

    case SetupMsg(relationName) => {
      become(working(relationName))
      //request tuples from the master
      sender ! RequestTuples()
      flowControlMessages +=1
    }
  }

  def working(relationName : String): Receive = {
    case UpdateMessage(tuple : List[String], key:String, ts:Int) => {
      log.debug("Worker: received a tuple")
      //insert the tuple in the view
      view.addBinding(key,tuple)

      //update the end time and counter of processed messages
      endTime = System.nanoTime
      processedMsgs +=1

      //request more tuples from the master
      sender ! RequestTuples()
    }

    case UpdateMessageBatch(tuples : List[(List[String],String)], ts:Int) =>{
      log.debug("Worker: received a tuple")
      //insert the tuples in the view
      tuples.foreach{tuple =>
        view.addBinding(tuple._2,tuple._1)

        //update the end time and counter of processed messages
        endTime = System.nanoTime
        processedMsgs +=1
      }
      //request more tuples from the master
      flowControlMessages +=1
      if(flowControlMessages >= flowController) {
        sender ! RequestTuples()
        flowControlMessages = 0
      }
    }

    case PrintProgress => {
      val duration = (endTime-startTime)/1000000
      val throughput = processedMsgs.toFloat / (duration/1000).toFloat
      log.info(relationName+" :finished processing "+processedMsgs + " taking "+ duration+" ms , throughput so far: "+ throughput +" (msg/sec)")

      //context.stop(self)
    }
  }


  def register(member: Member):Unit = {
    if (member.hasRole("master")) {
      log.info("To send a request to master actor to register a new worker: " + self)

      context.actorSelection(RootActorPath(member.address) / "user" / "master") ! WorkerActorRegisteration

    }
  }
}
