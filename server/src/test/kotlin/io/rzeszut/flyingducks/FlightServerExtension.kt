package io.rzeszut.flyingducks

import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import java.net.ServerSocket

class FlightServerExtension : BeforeAllCallback, AfterAllCallback {

  val location: Location = Location.forGrpcInsecure("0.0.0.0", findPort())

  private val allocator = RootAllocator()
  private val database = DuckDatabase.createForUri("jdbc:duckdb:", allocator)
  private val serverImpl = FlyingDucksServer(allocator, database)
  private val server = FlightServer.builder(allocator, location, serverImpl).build()

  override fun beforeAll(context: ExtensionContext?) {
    server.start()
  }

  override fun afterAll(context: ExtensionContext?) {
    server.close()
    allocator.close()
    database.close()
  }

  companion object {
    fun findPort(): Int = ServerSocket(0).use { it.localPort }
  }
}