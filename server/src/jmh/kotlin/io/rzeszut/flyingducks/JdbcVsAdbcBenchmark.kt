package io.rzeszut.flyingducks

import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VarCharVector
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.net.ServerSocket
import java.sql.DriverManager

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
open class JdbcVsAdbcBenchmark {

  @State(Scope.Benchmark)
  open class Database {

    companion object {
      fun findPort(): Int = ServerSocket(0).use { it.localPort }
    }

    val location: Location = Location.forGrpcInsecure("0.0.0.0", findPort())
    val allocator = RootAllocator()
    private val serverAllocator = allocator.newChildAllocator("server", 0, allocatorSize)
    private val duckAllocator = allocator.newChildAllocator("duckdb", 0, allocatorSize)

    private val databaseFilePath = System.getProperty("jmh.databaseFile")
    private val database = DuckDatabase.createForUri("jdbc:duckdb:${databaseFilePath}", duckAllocator)
    private val serverImpl = FlyingDucksServer(serverAllocator, database)
    private val server = FlightServer.builder(serverAllocator, location, serverImpl).build()

    @Setup
    fun setup() {
      server.start()
    }

    @TearDown
    fun tearDown() {
      for (c in listOf(server, database, allocator)) {
        try {
          c.close()
        } catch (e: Throwable) {
          // don't throw from this method, just close silently
          e.printStackTrace()
        }
      }
    }

    fun port() = location.uri.port
  }

  companion object {
    val allocatorSize: Long = 1024 * 1024 * 1024 /* 1 GiB */;

    val query = """
      select trip_id,
             trip_type,
             pickup,
             dropoff,
             cab_type,
             payment_type,
             trip_distance,
             total_amount,
             passenger_count
        from trips;
      """.trimIndent()
  }

  @Benchmark
  @Threads(1)
  fun adbc(database: Database, blackhole: Blackhole) {
    FlightSqlDriver(database.allocator.newChildAllocator("adbc", 0, allocatorSize))
      .open(mapOf(AdbcDriver.PARAM_URI.key to database.location.uri.toString())).use { db ->
        db.connect().use { conn ->
          conn.createStatement().use { statement ->
            statement.setSqlQuery(query)
            statement.prepare()
            statement.executeQuery().reader.use { reader ->
              val root = reader.vectorSchemaRoot
              while (reader.loadNextBatch()) {
                for (vector in root.fieldVectors) {
                  when (vector) {
                    is BigIntVector -> for (i in 0..<vector.valueCount) {
                      blackhole.consume(vector[i])
                    }

                    is Float8Vector -> for (i in 0..<vector.valueCount) {
                      blackhole.consume(vector[i])
                    }

                    is VarCharVector -> for (i in 0..<vector.valueCount) {
                      blackhole.consume(String(vector[i], Charsets.UTF_8))
                    }
                  }
                }
              }
            }
          }
        }
      }
  }

  @Benchmark
  @Threads(1)
  fun jdbc(database: Database, blackhole: Blackhole) {
    val uri = "jdbc:arrow-flight-sql://localhost:${database.port()}?useEncryption=false"
    DriverManager.getConnection(uri).use { conn ->
      conn.createStatement().use { statement ->
        statement.executeQuery(query).use { rs ->
          while (rs.next()) {
            blackhole.consume(rs.getLong("trip_id"))
            blackhole.consume(rs.getString("trip_type"))
            blackhole.consume(rs.getString("pickup"))
            blackhole.consume(rs.getString("dropoff"))
            blackhole.consume(rs.getString("cab_type"))
            blackhole.consume(rs.getString("payment_type"))
            blackhole.consume(rs.getDouble("trip_distance"))
            blackhole.consume(rs.getDouble("total_amount"))
            blackhole.consume(rs.getLong("passenger_count"))
          }
        }
      }
    }
  }
}