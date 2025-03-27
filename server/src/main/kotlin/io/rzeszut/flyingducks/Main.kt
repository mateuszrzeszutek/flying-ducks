package io.rzeszut.flyingducks

import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.RootAllocator

object Main {

  @JvmStatic
  fun main(args: Array<String>) {
    // todo slf4j!

    val allocator = RootAllocator()
    val location = Location.forGrpcInsecure("0.0.0.0", 7777)

    allocator.use {
      val serverImpl = FlyingDucksServer(allocator)
      FlightServer.builder(allocator, location, serverImpl).build().use {
        it.start()
        it.awaitTermination()
      }
    }
  }
}