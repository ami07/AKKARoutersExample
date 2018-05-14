package akkarouting.core


/**
  * based on the implementation of consistent hashmapping listed in https://github.com/akka/akka/blob/master/akka-actor/src/main/scala/akka/routing/ConsistentHashing.scala
  */

import akka.routing.RoutingLogic
import scala.collection.immutable
import akka.dispatch.Dispatchers
import com.typesafe.config.Config
import akka.actor.SupervisorStrategy
import akka.japi.Util.immutableSeq
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.ActorSystem
import java.util.concurrent.atomic.AtomicReference
import akka.serialization.SerializationExtension
import scala.util.control.NonFatal
import akka.event.Logging
import akka.actor.ActorPath


/*object SimpleHashingRouter {

  trait SimpleHashable {
    def simpleHashKey: Any
  }

  type SimpleHashMapping = PartialFunction[Any, Any]


  object emptySimpleHashMapping extends SimpleHashMapping {
    def isDefinedAt(x: Any) = false
    def apply(x: Any) = throw new UnsupportedOperationException("Empty ConsistentHashMapping apply()")
  }


  trait SimpleHashMapper {
    def hashKey(message: Any): Any
  }

  /**
    * INTERNAL API
    */
  private[akka] def hashMappingAdapter(mapper: SimpleHashMapper): SimpleHashMapping = {
    case message if mapper.hashKey(message).asInstanceOf[AnyRef] ne null ⇒
      mapper.hashKey(message)
  }
}

object ConsistentHashingRoutingLogic {
  /**
    * Address to use for the selfAddress parameter
    */
  def defaultAddress(system: ActorSystem): Address =
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
}


class SimpleHashRoutingLogic(system: ActorSystem, virtualNodesFactor: Int= 0, hashMapping:SimpleHashingRouter.SimpleHashMapping = SimpleHashingRouter.emptySimpleHashMapping) extends RoutingLogic) {

  import SimpleHashingRouter._
  def this(system: ActorSystem) =  this(system, virtualNodesFactor = 0, hashMapping = SimpleHashingRouter.emptySimpleHashMapping)

  private lazy val selfAddress = {
    // Important that this is lazy, because consistent hashing routing pool is used by SimpleDnsManager
    // that can be activated early, before the transport defaultAddress is set in the startup.
    // See issue #20263.
    // If defaultAddress is not available the message will not be routed, but new attempt
    // is performed for next message.
    val a = SimpleHashingRoutingLogic.defaultAddress(system)
    if (a == null)
      throw new IllegalStateException("defaultAddress not available yet")
    a
  }

  val vnodes =
  if (virtualNodesFactor == 0) system.settings.DefaultVirtualNodesFactor
  else virtualNodesFactor

  private lazy val log = Logging(system, getClass)



}*/



