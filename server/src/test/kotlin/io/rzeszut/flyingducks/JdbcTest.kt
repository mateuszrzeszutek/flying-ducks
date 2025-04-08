package io.rzeszut.flyingducks

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.extension.RegisterExtension
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContentEquals

class JdbcTest {

  companion object {

    @RegisterExtension
    private val server = FlightServerExtension()

    private lateinit var connection: Connection

    @JvmStatic
    @BeforeAll
    fun initialize() {
      connection =
        DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:${server.location.uri.port}?useEncryption=false")

      connection.createStatement().use {
        it.executeUpdate("CREATE TABLE people (name VARCHAR NOT NULL, age INTEGER, country VARCHAR);")
        it.executeUpdate(
          """
          INSERT INTO people (name, age, country)
                      VALUES ('John', 35, 'UK'),
                             ('Hans', 42, 'DE'),
                             ('Piotr', 21, 'PL'),
                             ('Andrzej', 37, 'PL');
        """.trimIndent()
        )
      }
    }

    @JvmStatic
    @AfterAll
    fun destroy() = connection.close()
  }

  @Test
  fun `should read database metadata - tables`() {
    // given
    data class Table(val catalog: String, val schema: String, val table: String, val tableType: String)

    // when
    val tables = mutableListOf<Table>()
    connection.metaData.getTables(null, null, null, arrayOf()).use { rs ->
      while (rs.next()) {
        tables += Table(
          rs.getString("TABLE_CAT"),
          rs.getString("TABLE_SCHEM"),
          rs.getString("TABLE_NAME"),
          rs.getString("TABLE_TYPE")
        )
      }
    }

    // then
    assertContentEquals(
      listOf(Table("memory", "main", "people", "BASE TABLE")),
      tables
    )
  }

  @Test
  fun `should read database metadata - columns`() {
    // given
    data class Column(val name: String, val type: String, val nullable: Boolean)

    // when
    val columns = mutableListOf<Column>()
    connection.metaData.getColumns("memory", "main", "people", null).use { rs ->
      while (rs.next()) {
        columns += Column(
          rs.getString("COLUMN_NAME"),
          rs.getString("TYPE_NAME"),
          rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable
        )
      }
    }

    // then
    assertContentEquals(
      listOf(
        Column("name", "VARCHAR", false),
        Column("age", "BIGINT", true),
        Column("country", "VARCHAR", true),
      ),
      columns
    )
  }

  @Test
  fun `should query database`() {
    // given
    data class Result(val country: String, val averageAge: Double)

    // when
    val results = mutableListOf<Result>()
    connection.prepareStatement("SELECT country, AVG(age) FROM people WHERE country <> ? GROUP BY country ORDER BY 2 ASC;")
      .use { ps ->
        ps.setString(1, "UK")
        ps.executeQuery().use { rs ->
          while (rs.next()) {
            results += Result(
              rs.getString(1),
              rs.getDouble(2)
            )
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