package akkarouting.local

import akka.actor.{ActorSystem, Props}
import akkarouting.core.WorkerActor.{PrintProgress, SetupMsg, UpdateMessage}
import akkarouting.core.{FileParser, WorkerActor}
import com.typesafe.config.ConfigFactory

import scala.io.Source

object NoRouterMain {

  def main(args: Array[String]): Unit = {
    //load config file
    val config = ConfigFactory.load()

    //create an actor system and and three actors, one for each table
    val system: ActorSystem = ActorSystem("AKKANoRouterLocal")
    val simpleActor_L = system.actorOf(Props[WorkerActor], "actorStoreL")
//    val simpleActor_PS = system.actorOf(Props[WorkerActor], "actorStorePS")
//    val simpleActor_S = system.actorOf(Props[WorkerActor], "actorStoreS")

    //setup actors
    simpleActor_L ! SetupMsg("L")
//    simpleActor_S ! SetupMsg("S")
//    simpleActor_PS ! SetupMsg("PS")
    Thread.sleep(3000)


    //get the start time of the reading
    val startTime = System.nanoTime

    //read file with streamed data
    val inputFileName = config.getString("routingexample.filestream")

    val streamFile = Source.fromFile(inputFileName, "UTF-8")

    val numInsertions = config.getInt("routingexample.numInsertions")
    val limitInsertions = config.getBoolean("routingexample.limitInsertions")

    val streamInsertionLines = if (limitInsertions) {
      streamFile.getLines().take(numInsertions)
    } else {
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
        case "L" => simpleActor_L ! UpdateMessage(tuple, processedLines)

        /*case "PS" => simpleActor_PS ! UpdateMessage(tuple, processedLines)

        case "S" => simpleActor_S ! UpdateMessage(tuple, processedLines)*/

        case _ =>
      }
    }

    //print progress at each actor
    simpleActor_L ! PrintProgress
//    simpleActor_PS ! PrintProgress
//    simpleActor_S ! PrintProgress

    //get the end time of the reading
    val endTime = System.nanoTime

    //print the duration
    val duration = (endTime - startTime) / 1000000
    println("reading the file and sending messages to actors took " + duration + " ms")
  }
}
