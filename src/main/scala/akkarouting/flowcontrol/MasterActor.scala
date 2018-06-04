package akkarouting.flowcontrol

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{FileIO, Sink, Source, StreamRefs}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import akkarouting.core.WorkerActorCF.{PrintProgress, SetupMsg, UpdateMessage, UpdateMessageBatch}
import akkarouting.core.{FileParser, WorkerActorCF}
import akkarouting.flowcontrol.MasterActor.{ProcessStream, RequestTuples, WorkerActorRegisteration}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
//import scala.io.Source

object MasterActor{
  case object ProcessStream
  case object WorkerActorRegisteration
  case class RequestTuples(/*childRelation : String*/)
}


class MasterActor extends Actor with ActorLogging{

  import context.dispatcher
  import akka.stream.SourceRef
  import akka.pattern.pipe

  implicit val mat = ActorMaterializer()(context)

  val config = ConfigFactory.load()
  //list of registered workers
  var backendWorkerActors = IndexedSeq.empty[ActorRef]
  var numRegisteredWorkers = 0

  val numRoutees = config.getInt("routingexample.numRoutees")
  val numViews = config.getInt("routingexample.numViews")
  val neededNumberOfWorkers = numRoutees * numViews

  var workerActor: ActorRef = null


  //read file with streamed data
  val inputFileName = config.getString("routingexample.filestream")

  val streamFile = scala.io.Source.fromFile(inputFileName, "UTF-8")

  val numInsertions = config.getInt("routingexample.numInsertions")
  val limitInsertions = config.getBoolean("routingexample.limitInsertions")

  val streamInsertionLines = if(limitInsertions) {
    streamFile.getLines().take(numInsertions)
  }else{
    streamFile.getLines()
  }
  val batchLength = config.getInt("routingexample.batchLength")

  var line : String = null
  var processedLines = 0

  var startTime = System.nanoTime

  override def receive: Receive = {
    case WorkerActorRegisteration =>{
      log.info("received a new worker node to register: "+sender())
      if(!backendWorkerActors.contains(sender())){
        log.info("a new worker to join the execution of the query: "+sender())
        //watch the sender
        context watch sender()
        //add the sender to the list of workers
        backendWorkerActors = backendWorkerActors :+ sender()
        numRegisteredWorkers +=1
        log.info("added "+sender() +" Workers in the cluster so far: "+backendWorkerActors)
      }

      //if the number of registered workers reached the required number, start execution
      if(numRegisteredWorkers==neededNumberOfWorkers){
        log.info("We have received enough workers to start the execution "+numRegisteredWorkers)
//        self ! ProcessStream

        //get the start time of the reading
        startTime = System.nanoTime

        //now setup the router and routees
//        log.info("Use my SimpleHashRouter")
        //val simpleRouter_L = context.actorOf(SimpleHashRouter.props("simpleRouter_L",backendWorkerActors.toList/*backendWorkerActorsPart(1)*/),name = "simpleRouter_L")

        workerActor = backendWorkerActors.head //context.actorOf(Props[WorkerActorCF], name = "workerActor")
        workerActor ! SetupMsg("L")
        Thread.sleep(3000)
      }else{
        log.info("We now have "+numRegisteredWorkers+" registered worker actors, wait them to reach "+neededNumberOfWorkers)
      }
    }

    /*case RequestTuples(childRelation) => {
      //read L tuple from source
      if(streamInsertionLines.hasNext){
        processedLines += 1
        //parse the line
        line = streamInsertionLines.next()
        val (relationName, tuple) = FileParser.parse(line)

        val ts_tuple = processedLines.toString + "*" + line
        val source: Source[String,  NotUsed] = akka.stream.scaladsl.Source(tuple)

        // materialize the SourceRef:
        val ref: Future[SourceRef[String]] = source.runWith(StreamRefs.sourceRef())

        // wrap the SourceRef in some domain message, such that the sender knows what source it is
        //val reply: Future[UpdateMessage] = ref.map(UpdateMessage(relationName, _))

        // reply to sender
        //reply pipeTo sender()

      }else{
        //we have finished reading all the lines in the stream file -- send a finish message to the worker
        //workerActor ! PrintProgress
      }

//      val source: Source[ByteString, Future[IOResult]] =  FileIO.fromPath(scala.io.Source(inputFileName))


    }*/

    case RequestTuples(/*childRelation*/) => {
      log.debug("Master: received a request to send tuple")
      //read L tuple from source
      if(streamInsertionLines.hasNext) {
        processedLines += 1

        //get enugh tuples to fill the batch
        var num = 0
        val batch: ListBuffer[(List[String], String)] = ListBuffer.empty
        var tuplePair : (List[String],String) = null
        while(num < batchLength && streamInsertionLines.hasNext) {
          //parse the line
          line = streamInsertionLines.next()
          val (relationName, tuple) = FileParser.parse(line)

          //send a message to worker
          relationName match {
            case "L" => {
              tuplePair = (tuple, tuple(2))
              batch +=  tuplePair
            } //sender ! UpdateMessage(tuple, tuple(2), processedLines)
            case "PS" => {
              tuplePair = (tuple, tuple(1))
              batch +=  tuplePair
            }//sender ! UpdateMessage(tuple, tuple(1), processedLines)
            case "S" => {
              tuplePair = (tuple, tuple(0))
              batch +=  tuplePair
            }//sender ! UpdateMessage(tuple, tuple(0), processedLines)
          }
        }
        //end the batch to sender
        sender ! UpdateMessageBatch(batch.toList,processedLines)

      }else{
        log.debug("Master: stream file ended, print progress")
        //we have finished reading all the lines in the stream file -- send a finish message to the worker
        workerActor ! PrintProgress
      }
    }

  }
}
