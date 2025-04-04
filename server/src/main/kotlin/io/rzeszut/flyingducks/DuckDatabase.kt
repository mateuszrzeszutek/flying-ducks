package io.rzeszut.flyingducks

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBResultSet
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ParameterMetaData
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import javax.annotation.concurrent.NotThreadSafe

// this is **NOT** thread-safe, we're using a single connection over and over again without any sort of synchronization
@NotThreadSafe
class DuckDatabase private constructor(private val connection: Connection) : AutoCloseable {

  companion object {
    fun create(filePath: Path) = DuckDatabase(DriverManager.getConnection("jdbc:duckdb:$filePath"))
  }

  override fun close() {
    connection.close()
  }

  fun prepare(sql: String) = Statement(connection.prepareStatement(sql))

  class Statement(private val preparedStatement: PreparedStatement) : AutoCloseable {

    fun executeQuery(allocator: BufferAllocator): ArrowReader {
      val resultSet = preparedStatement.executeQuery() as DuckDBResultSet
      return resultSet.arrowExportStream(allocator, 256) as ArrowReader
    }

    fun resultSetMetaData(): ResultSetMetaData = preparedStatement.metaData

    fun parameterMetaData(): ParameterMetaData = preparedStatement.parameterMetaData

    override fun close() {
      preparedStatement.close()
    }
  }
}