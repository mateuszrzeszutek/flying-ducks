package io.rzeszut.flyingducks

import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.RootAllocator
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.system.exitProcess

object Main {

  private val log = LoggerFactory.getLogger(Main::class.java)

  @JvmStatic
  fun main(args: Array<String>) {
    if (args.isEmpty()) {
      log.error("Error: path to the database file is required.")
      exitProcess(1)
    }
    val duckDbFilePath = Paths.get(args.first())
    if (Files.notExists(duckDbFilePath)) {
      log.error("Error: the database file $duckDbFilePath does not exist.")
      exitProcess(1)
    }

    RootAllocator().use { allocator ->
      DuckDatabase.create(duckDbFilePath, allocator).use { database ->
        val location = Location.forGrpcInsecure("0.0.0.0", 7777)
        val serverImpl = FlyingDucksServer(allocator, database)
        FlightServer.builder(allocator, location, serverImpl).build().use { server ->
          log.info("Starting the Flying Ducks server ...")
          server.start()
          server.awaitTermination()
        }
      }
    }
  }
}