package io.rzeszut.flyingducks

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.extension.RegisterExtension
import java.sql.DatabaseMetaData
import kotlin.test.Test
import kotlin.test.assertContentEquals

class AdbcTest {

  companion object {

    @RegisterExtension
    private val server = FlightServerExtension()

    private val allocator = RootAllocator()

    private lateinit var database: AdbcDatabase

    @JvmStatic
    @BeforeAll
    fun initialize() {
      database = FlightSqlDriver(allocator)
        .open(mapOf(AdbcDriver.PARAM_URI.key to server.location.uri.toString()))

      database.connect().use { conn ->
        conn.createStatement().use { statement ->
          statement.setSqlQuery("CREATE TABLE people (name VARCHAR NOT NULL, age INTEGER, country VARCHAR);")
          statement.executeUpdate()

          statement.setSqlQuery(
            """
            INSERT INTO people (name, age, country)
                        VALUES ('John', 35, 'UK'),
                               ('Hans', 42, 'DE'),
                               ('Piotr', 21, 'PL'),
                               ('Andrzej', 37, 'PL');
          """.trimIndent()
          )
          statement.executeUpdate()
        }
      }
    }

    @JvmStatic
    @AfterAll
    fun destroy() {
      database.close()
      allocator.close()
    }
  }

  @Test
  fun `should read database metadata`() {
    // given
    data class Column(val name: String, val type: String, val nullable: Boolean)
    data class Table(
      val catalog: String,
      val schema: String,
      val table: String,
      val tableType: String,
      val columns: List<Column>
    )

    // when
    val foundTables = mutableListOf<Table>()
    database.connect().use { conn ->
      conn.getObjects(AdbcConnection.GetObjectsDepth.ALL, "memory", null, null, null, null).use { reader ->
        val root = reader.vectorSchemaRoot
        while (reader.loadNextBatch()) {
          for (i in 0..<root.rowCount) {
            val catalog = root.getString("catalog_name", i)
            val schemas = (root.getVector("catalog_db_schemas") as ListVector).getObject(0)
              .map { it as Map<String, Any> }
            for (schema in schemas) {
              val schemaName = schema["db_schema_name"].toString()
              val tables = schema["db_schema_tables"] as List<Map<String, Any>>
              for (table in tables) {
                val columns = (table["table_columns"] as List<Map<String, Any>>).map { col ->
                  Column(
                    col["column_name"].toString(),
                    col["xdbc_type_name"].toString(),
                    (col["xdbc_nullable"] as Number).toInt() == DatabaseMetaData.columnNullable
                  )
                }

                foundTables += Table(
                  catalog,
                  schemaName,
                  table["table_name"].toString(),
                  table["table_type"].toString(),
                  columns
                )
              }
            }
          }
        }
      }
    }

    // then
    println(foundTables)
    assertContentEquals(
      listOf(
        Table(
          "memory", "main", "people", "BASE TABLE", listOf(
            Column("name", "VARCHAR", false),
            Column("age", "BIGINT", true),
            Column("country", "VARCHAR", true),
          )
        )
      ),
      foundTables
    )
  }

  @Test
  fun `should query database`() {
    // given
    data class Result(val country: String, val averageAge: Double)

    // when
    val results = mutableListOf<Result>()
    database.connect().use { conn ->
      conn.createStatement().use { statement ->
        VectorSchemaRoot.of(VarCharVector("", allocator)).use { params ->
          params.setString("", 0, "UK")
          params.rowCount = 1

          statement.setSqlQuery("SELECT country, AVG(age) FROM people WHERE country <> ? GROUP BY country ORDER BY 2 ASC;")
          statement.prepare()
          statement.bind(params)
          statement.executeQuery().reader.use { reader ->
            val root = reader.vectorSchemaRoot
            while (reader.loadNextBatch()) {

              for (i in 0..<root.rowCount) {
                results += Result(
                  root.getString(0, i),
                  root.getDouble(1, i)
                )
              }
            }
          }
        }
      }
    }

    // then
    assertContentEquals(
      listOf(
        Result("PL", 29.0),
        Result("DE", 42.0)
      ),
      results
    )
  }
}

fun VectorSchemaRoot.getDouble(fieldIndex: Int, index: Int) =
  (getVector(fieldIndex) as Float8Vector)[index]

fun VectorSchemaRoot.getString(fieldIndex: Int, index: Int) =
  String((getVector(fieldIndex) as VarCharVector)[index], Charsets.UTF_8)
