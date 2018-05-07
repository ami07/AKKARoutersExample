package akkarouting.core

import akka.actor.{Actor, ActorLogging, RootActorPath}
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{Cluster, Member}
import akkarouting.cluster.MasterActor.WorkerActorRegisteration
import akkarouting.core.WorkerActor.{PrintProgress, SetupMsg, UpdateMessage}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.{HashMap, MultiMap,Set}

object WorkerActor{
  case class UpdateMessage(tuple : List[String], ts:Int)
  case class SetupMsg(relationName : String)
  case object PrintProgress
}
class WorkerActor extends Actor with ActorLogging{
  import context._

  //view maintained by this actor
  val view  = new HashMap[String,Set[List[String]]] with  MultiMap[String,List[String]]

  //counters to keep track of time and processed messages
  val startTime = System.nanoTime
  var endTime = System.nanoTime
  var processedMsgs = 0
  var finishedChildren = 0;
  var duplicated = 0


  val config = ConfigFactory.load()

  val isLocalProvider = if(config.getString("akka.actor.provider")=="local") true else false
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

    case SetupMsg(relationName) => become(working(relationName))
  }

  def working(relationName : String): Receive = {
    case UpdateMessage(tuple,ts) => {
      //extract the key from the tuple
      val keyIndex = relationName match {
        case "L"  => 2
        case "PS" => 1
        case "S"  => 0
        case _    => 0
      }

      //adding O(n) processing
      val viewList = view.toList
      viewList.find(e => e._1==tuple(keyIndex))
      for(e <- viewList){
        if(e._1==tuple(keyIndex)) duplicated+=1
      }

      //insert the tuple in the view
      view.addBinding(tuple(keyIndex),tuple)

      //update the end time and counter of processed messages
      endTime = System.nanoTime
      processedMsgs +=1
    }

    case PrintProgress => {
      val duration = (endTime-startTime)/1000000
      val throughput = processedMsgs.toFloat / (duration/1000).toFloat
      log.info(relationName+" :finished processing "+processedMsgs + " taking "+ duration+" ms , throughput so far: "+ throughput +" (msg/sec)")

      context.stop(self)
    }
  }


  def register(member: Member):Unit = {
    if (member.hasRole("master")) {
      log.info("To send a request to master actor to register a new worker: " + self)

      context.actorSelection(RootActorPath(member.address) / "user" / "master") ! WorkerActorRegisteration

    }
  }
}
