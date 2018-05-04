package akkarouting.local

import akka.actor.{ActorSystem, Props}
import akka.routing.{Broadcast, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akkarouting.core.{FileParser, WorkerActor}
import akkarouting.core.WorkerActor.{PrintProgress, SetupMsg, UpdateMessage}
import com.typesafe.config.ConfigFactory

import scala.io.Source

object RouterPoolMain {

  def main(args: Array[String]): Unit = {
    //load config file
    val config = ConfigFactory.load()

    def hashMappingPartL: ConsistentHashMapping = {
      case UpdateMessage(tuple,ts) => {
        tuple(2)
      }
    }

    def hashMappingPartPS: ConsistentHashMapping = {
      case UpdateMessage(tuple,ts) => {
        tuple(1)
      }
    }

    def hashMappingPartS: ConsistentHashMapping = {
      case UpdateMessage(tuple,ts) => {
        tuple(0)
      }
    }


    //create routers with pools local to this m/c
    val system: ActorSystem = ActorSystem("AKKARouterLocal")
    val numRoutees = config.getInt("routingexample.numRoutees")
    val simpleRouter_L = system.actorOf(
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartL).props(Props[WorkerActor].withDispatcher("akka.actor.my-pinned-dispatcher")),name = "simpleHashPoolRouterL")
      ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartL).props(Props[WorkerActor].withDispatcher("akka.actor.worker-dispatcher")),name = "simpleHashPoolRouterL")
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartL).props(Props[WorkerActor].withDispatcher("akka.actor.workerMM-dispatcher")),name = "simpleHashPoolRouterL")

    val simpleRouter_S = system.actorOf(
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartS).props(Props[WorkerActor].withDispatcher("akka.actor.my-pinned-dispatcher")),name = "simpleHashPoolRouterS")
      ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartS).props(Props[WorkerActor].withDispatcher("akka.actor.worker-dispatcher")),name = "simpleHashPoolRouterS")
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartS).props(Props[WorkerActor].withDispatcher("akka.actor.workerMM-dispatcher")),name = "simpleHashPoolRouterS")

    val simpleRouter_PS = system.actorOf(
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartPS).props(Props[WorkerActor].withDispatcher("akka.actor.my-pinned-dispatcher")),name = "simpleHashPoolRouterPS")
      ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartPS).props(Props[WorkerActor].withDispatcher("akka.actor.worker-dispatcher")),name = "simpleHashPoolRouterPS")
      //ConsistentHashingPool(numRoutees, virtualNodesFactor =numRoutees, hashMapping= hashMappingPartPS).props(Props[WorkerActor].withDispatcher("akka.actor.workerMM-dispatcher")),name = "simpleHashPoolRouterPS")


    //setup  actors
    simpleRouter_L ! Broadcast(SetupMsg("L"))
    simpleRouter_S ! Broadcast(SetupMsg("S"))
    simpleRouter_PS ! Broadcast(SetupMsg("PS"))
    Thread.sleep(3000)


    //get the start time of the reading
    val startTime = System.nanoTime

    //read file with streamed data
    val inputFileName = config.getString("routingexample.filestream")

    val streamFile = Source.fromFile(inputFileName, "UTF-8")

    val numInsertions = config.getInt("routingexample.numInsertions")
    val limitInsertions = config.getBoolean("routingexample.limitInsertions")

    val streamInsertionLines = if(limitInsertions) {
      streamFile.getLines().take(numInsertions)
    }else{
      streamFile.getLines()
    }

    //keep track of the update number in the stream file
    var processedLines = 0

    //for each line in the stream, send a message to the corresponding actor
    for (l <- streamInsertionLines) {
      //parse the line
      val (relationName, tuple) = FileParser.parse(l)
      processedLines += 1

      relationName match {
        case "L" => simpleRouter_L ! UpdateMessage(tuple, processedLines)

        case "PS" => simpleRouter_PS ! UpdateMessage(tuple, processedLines)

        case "S" => simpleRouter_S ! UpdateMessage(tuple, processedLines)

        case _ =>
      }
    }

    //broadcast to all actors to print the progress they made
    simpleRouter_L !  Broadcast(PrintProgress)
    simpleRouter_PS !  Broadcast(PrintProgress)
    simpleRouter_S !  Broadcast(PrintProgress)

    //get the end time of the reading
    val endTime = System.nanoTime

    //print the duration
    val duration = (endTime-startTime)/1000000
    println("reading the file and sending messages to actors took "+duration+" ms")


  }
}
