package akkarouting.flowcontrol

import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import akkarouting.core.WorkerActorCF
import com.typesafe.config.ConfigFactory

class Main extends App {

  //load the config
  val config = ConfigFactory.load()

  if (config.getStringList("akka.cluster.roles").contains("master")) {

    val system = ActorSystem("AKKARouterCluster")
    println(s"Starting node with roles: ${Cluster(system).selfRoles}")

    Cluster(system).registerOnMemberUp {
      //start the master actor
      system.actorOf(Props[MasterActor],name = "master")
      println("Master actor is ready to execute the query ")
    }
  }else if (config.getStringList("akka.cluster.roles").contains("worker")) {
    //create the number of specified actors for this node of the cluster

    val numWorkersPerNode = config.getInt("routingexample.numWorkersPerNode")
    val system = ActorSystem("AKKARouterCluster")
    println(s"To start $numWorkersPerNode worker actors")
    for(i<-0 until numWorkersPerNode) {
      system.actorOf(Props[WorkerActorCF], name = s"worker:$i")
    }

  }else{
    //this is probably a seed and we still need to create an actor system for it
    val system = ActorSystem("AKKARouterCluster")
  }
}
