package akkarouting.cluster

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.routing.{Broadcast, ConsistentHashingGroup}
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akkarouting.core.WorkerActor
//import akkarouting.core.SimpleHashRouter.{PrintProgress, SetupMsg, UpdateMessage}
import akkarouting.core.{FileParser, SimpleHashRouter}
//import akkarouting.core.WorkerActor.{PrintProgress, SetupMsg, UpdateMessage}
import com.typesafe.config.ConfigFactory

import scala.io.Source

object MasterActor{
  case object ProcessStream
  case object WorkerActorRegisteration
}

class MasterActor extends Actor with ActorLogging{
  import akkarouting.cluster.MasterActor._

  val config = ConfigFactory.load()
  //list of registered workers
  var backendWorkerActors = IndexedSeq.empty[ActorRef]
  var numRegisteredWorkers = 0

  val numRoutees = config.getInt("routingexample.numRoutees")
  val numViews = config.getInt("routingexample.numViews")
  val neededNumberOfWorkers = numRoutees * numViews
  val myRouter = config.getString("routingexample.routerType").toLowerCase.equals("shr")


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
        self ! ProcessStream
      }else{
        log.info("We now have "+numRegisteredWorkers+" registered worker actors, wait them to reach "+neededNumberOfWorkers)
      }
    }

    case ProcessStream =>{
      log.info("got enough workers -- start executing query")

      def hashMappingPartL: ConsistentHashMapping = {
        case WorkerActor.UpdateMessage(tuple,ts) => {
          tuple(2)
        }
      }

      /*def hashMappingPartPS: ConsistentHashMapping = {
        case UpdateMessage(tuple,ts) => {
          tuple(1)
        }
      }

      def hashMappingPartS: ConsistentHashMapping = {
        case UpdateMessage(tuple,ts) => {
          tuple(0)
        }
      }*/

      //function to split a list into n sub-lists
      def split[A](xs: List[A], n: Int): List[List[A]] = {
        if (xs.isEmpty) Nil
        else {
          val (ys, zs) = xs.splitAt(n)
          ys :: split(zs, n)
        }
      }

      //read file with streamed data
      val inputFileName = config.getString("routingexample.filestream")

      val streamFile = Source.fromFile(inputFileName, "UTF-8")

      val numInsertions = config.getInt("routingexample.numInsertions")
      val limitInsertions = config.getBoolean("routingexample.limitInsertions")


      //create routers
      //split the workers into three groups
      val backendWorkerActorsPart: List[List[ActorRef]] = split(backendWorkerActors.toList,numRoutees)

      //get the start time of the reading
      val startTime = System.nanoTime

      //create three routers, one for each table
      if(myRouter){
        log.info("Use my SimpleHashRouter")
        val simpleRouter_L = context.actorOf(SimpleHashRouter.props("simpleRouter_L",backendWorkerActors.toList/*backendWorkerActorsPart(1)*/),name = "simpleRouter_L")
        //setup actors
        simpleRouter_L ! SimpleHashRouter.SetupMsg("L")
        /*simpleRouter_S ! SetupMsg("S")
        simpleRouter_PS ! SetupMsg("PS")*/
        Thread.sleep(3000)



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
            case "L" => simpleRouter_L ! SimpleHashRouter.UpdateMessage(tuple, tuple(0)+tuple(1)+tuple(2), processedLines)

            /*case "PS" => simpleRouter_PS ! UpdateMessage(tuple, tuple(0)+tuple(1),processedLines)

            case "S" => simpleRouter_S ! UpdateMessage(tuple,tuple(0), processedLines)*/

            case _ =>
          }
        }

        //broadcast to all actors to print the progress they made
        simpleRouter_L !  SimpleHashRouter.PrintProgress
        /*simpleRouter_PS !  PrintProgress
        simpleRouter_S !  PrintProgress*/

      }else {
        log.info("Use ConsistentHashingGroup")
        val simpleRouter_L = context.actorOf(ConsistentHashingGroup(backendWorkerActors.toList.map(a => a.path.toString),hashMapping=hashMappingPartL).props(),name = "simpleRouter_L" )
        //setup actors
        simpleRouter_L ! Broadcast(WorkerActor.SetupMsg("L"))
        /*simpleRouter_S ! SetupMsg("S")
        simpleRouter_PS ! SetupMsg("PS")*/
        Thread.sleep(3000)


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
            case "L" => simpleRouter_L ! WorkerActor.UpdateMessage(tuple,  processedLines)

            /*case "PS" => simpleRouter_PS ! UpdateMessage(tuple, tuple(0)+tuple(1),processedLines)

            case "S" => simpleRouter_S ! UpdateMessage(tuple,tuple(0), processedLines)*/

            case _ =>
          }
        }

        //broadcast to all actors to print the progress they made
        simpleRouter_L !  Broadcast(WorkerActor.PrintProgress)
        /*simpleRouter_PS !  PrintProgress
        simpleRouter_S !  PrintProgress*/
      }
     /* val simpleRouter_PS = context.actorOf(SimpleHashRouter.props("simpleRouter_PS",backendWorkerActorsPart(0)),name = "simpleRouter_PS")
      //context.actorOf(ConsistentHashingGroup(backendWorkerActorsPart(1).map(a => a.path.toString),hashMapping=hashMappingPartPS).props(),name = "simpleRouter_PS" )

      val simpleRouter_S = context.actorOf(SimpleHashRouter.props("simpleRouter_S",backendWorkerActorsPart(2)),name = "simpleRouter_S")
      //context.actorOf(ConsistentHashingGroup(backendWorkerActorsPart(2).map(a => a.path.toString),hashMapping=hashMappingPartS).props(),name = "simpleRouter_S" )
*/


      //get the end time of the reading
      val endTime = System.nanoTime

      //print the duration
      val duration = (endTime-startTime)/1000000
      println("reading the file and sending messages to actors took "+duration+" ms")


    }
  }
}
