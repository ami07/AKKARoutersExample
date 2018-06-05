package akkarouting.flowcontrol

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{FileIO, Sink, Source, StreamRefs}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import akkarouting.core.SimpleHashRouter.{PrintProgressCF, SetupMsgCF, UpdateMessageCF}
import akkarouting.core.WorkerActorCF.{PrintProgress, SetupMsg, UpdateMessage, UpdateMessageBatch}
import akkarouting.core.{FileParser, SimpleHashRouter, WorkerActorCF}
import akkarouting.flowcontrol.MasterActor.{ProcessStream, RequestTuples, RequestTuplesR, WorkerActorRegisteration}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, MultiMap,HashMap,Set}
import scala.concurrent.Future
//import scala.io.Source

object MasterActor{
  case object ProcessStream
  case object WorkerActorRegisteration
  case class RequestTuples(/*childRelation : String*/)
  case class RequestTuplesR(/*childRelation : String*/)
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
  var simpleRouter_L : ActorRef = null


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
  val flowController = config.getInt("routingexample.flowController")

  var line : String = null
  var processedLines = 0
  var printProgressSentFlag = false

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

        if(numRoutees==1) {
          log.info("one routee -- do not need a router")
          workerActor = backendWorkerActors.head //context.actorOf(Props[WorkerActorCF], name = "workerActor")
          workerActor ! SetupMsg("L")
          Thread.sleep(3000)
        }else{
          log.info("Use my SimpleHashRouter")
          simpleRouter_L = context.actorOf(SimpleHashRouter.props("simpleRouter_L",backendWorkerActors.toList/*backendWorkerActorsPart(1)*/),name = "simpleRouter_L")
          //setup actors
          simpleRouter_L ! SetupMsgCF("L",flowController)
          /*simpleRouter_S ! SetupMsg("S")
          simpleRouter_PS ! SetupMsg("PS")*/
          Thread.sleep(3000)
        }
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
      log.debug("Master - RequestTuples: received a request to send tuple")
      //read L tuple from source
      if(streamInsertionLines.hasNext) {


        var numMessages = 0

        while(numMessages < flowController && streamInsertionLines.hasNext) {
          log.debug("to create a batch and send it to worker -- numMessages sent so far for this pull request : "+numMessages+ "flowcontroller val: "+flowController)
          //get enugh tuples to fill the batch
          var num = 0
          val batch: ListBuffer[(List[String], String)] = ListBuffer.empty
          var tuplePair: (List[String], String) = null
          while (num < batchLength && streamInsertionLines.hasNext) {
            //parse the line
            line = streamInsertionLines.next()
            processedLines += 1
            val (relationName, tuple) = FileParser.parse(line)

            //send a message to worker
            relationName match {
              case "L" => {
                tuplePair = (tuple, tuple(2))
                batch += tuplePair
              } //sender ! UpdateMessage(tuple, tuple(2), processedLines)
              case "PS" => {
                tuplePair = (tuple, tuple(1))
                batch += tuplePair
              } //sender ! UpdateMessage(tuple, tuple(1), processedLines)
              case "S" => {
                tuplePair = (tuple, tuple(0))
                batch += tuplePair
              } //sender ! UpdateMessage(tuple, tuple(0), processedLines)
            }
            num += 1
          }
          //end the batch to sender
          sender ! UpdateMessageBatch(batch.toList, processedLines)

          numMessages +=1
        }
        log.debug("finished sending all message for this pull request")
        if(!streamInsertionLines.hasNext){
          //finished the stream file,
          log.info("Master: stream file ended, print progress")
          //we have finished reading all the lines in the stream file -- send a finish message to the worker
          workerActor ! PrintProgress
        }

      }else{
        log.debug("Master: stream file ended, print progress")
        //we have finished reading all the lines in the stream file -- send a finish message to the worker
        workerActor ! PrintProgress
      }
    }

    case RequestTuplesR(/*childRelation*/) => {
      log.info("Master - RequestTuplesR: received a request to send tuple")
      //read L tuple from source
      if(streamInsertionLines.hasNext) {


        var numMessages = 0
        var keyIndex:Int = 0

        while(numMessages < flowController && streamInsertionLines.hasNext) {
          log.debug("RequestTuplesR: to create a batch and send it to worker -- numMessages sent so far for this pull request : "+numMessages+ "flowcontroller val: "+flowController)
          //get enugh tuples to fill the batch
          var num = 0
          val batch = new HashMap[Int,Set[(List[String],String)]] with MultiMap[Int, (List[String],String)] //ListBuffer.empty
          var tuplePair: (List[String], String) = null
          while (num < batchLength && streamInsertionLines.hasNext) {
            //parse the line
            line = streamInsertionLines.next()
            processedLines += 1
            val (relationName, tuple) = FileParser.parse(line)

            //send a message to worker
            relationName match {
              case "L" => {
                tuplePair = (tuple, tuple(2))
                keyIndex = (tuple(2).toLong % numRoutees).toInt
                //batch.
                batch.addBinding(keyIndex,tuplePair)
              } //sender ! UpdateMessage(tuple, tuple(2), processedLines)
              case "PS" => {
                tuplePair = (tuple, tuple(1))
                keyIndex = (tuple(1).toLong % numRoutees).toInt
                batch.addBinding(keyIndex,tuplePair)
              } //sender ! UpdateMessage(tuple, tuple(1), processedLines)
              case "S" => {
                tuplePair = (tuple, tuple(0))
                keyIndex = (tuple(0).toLong % numRoutees).toInt
                batch.addBinding(keyIndex,tuplePair)
              } //sender ! UpdateMessage(tuple, tuple(0), processedLines)
            }
            num += 1
          }
          //end the batch to sender
          batch.foreach{b =>
            log.debug("RequestTuplesR: send a batch to worker for routee with index "+b._1+" batch size: "+ b._2.size)
            simpleRouter_L ! UpdateMessageCF(b._2.toList, b._1, processedLines)
          }


          numMessages +=1
        }
        log.debug("finished sending all message for this pull request")
        if(!streamInsertionLines.hasNext && !printProgressSentFlag){
          //finished the stream file,
          log.info("Master: stream file ended, print progress")
          //we have finished reading all the lines in the stream file -- send a finish message to the worker
          simpleRouter_L ! PrintProgressCF
          printProgressSentFlag = true
        }

      }else{
        if(!printProgressSentFlag) {
          log.info("Master: stream file ended, print progress")
          //we have finished reading all the lines in the stream file -- send a finish message to the worker
          simpleRouter_L ! PrintProgressCF
          printProgressSentFlag = true
        }
      }
    }

  }
}
