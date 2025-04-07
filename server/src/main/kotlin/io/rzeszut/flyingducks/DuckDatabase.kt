package io.rzeszut.flyingducks

import io.rzeszut.flyingducks.JdbcToArrow.sqlTypeToArrow
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.duckdb.DuckDBResultSet
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.sql.*
import javax.annotation.concurrent.NotThreadSafe

// this is **NOT** thread-safe, we're using a single connection over and over again without any sort of synchronization
@NotThreadSafe
class DuckDatabase private constructor(private val connection: Connection, private val allocator: BufferAllocator) :
  AutoCloseable {

  companion object {
    fun create(filePath: Path, allocator: BufferAllocator) =
      DuckDatabase(DriverManager.getConnection("jdbc:duckdb:$filePath"), allocator)
  }

  fun metadata() = DuckMetadata(this, allocator)

  fun prepare(sql: String): DuckStatement {
    return DuckStatement(connection.prepareStatement(sql), allocator)
  }

  override fun close() {
    connection.close()
  }
}

class DuckStatement(private val preparedStatement: PreparedStatement, private val allocator: BufferAllocator) :
  AutoCloseable {

  fun executeQuery(): QueryResult {
    val resultSet = preparedStatement.executeQuery() as DuckDBResultSet
    return QueryResult.forReader(resultSet.arrowExportStream(allocator, 1024) as ArrowReader)
  }

  fun closeOnQueryCompletion(): DuckStatement {
    preparedStatement.closeOnCompletion()
    return this
  }

  fun setParameter(index: Int, value: Any): DuckStatement {
    preparedStatement.setObject(index, value)
    return this
  }

  fun resultSetMetaData(): ResultSetMetaData = preparedStatement.metaData

  fun parameterMetaData(): ParameterMetaData = preparedStatement.parameterMetaData

  override fun close() {
    preparedStatement.close()
  }
}

class DuckMetadata(private val database: DuckDatabase, private val allocator: BufferAllocator) {

  fun getCatalogs(): QueryResult =
    database.prepare("SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name;")
      .closeOnQueryCompletion()
      .executeQuery()

  fun getSchemas(catalog: String?, schemaNamePattern: String?): QueryResult {
    val query = StringBuilder()
    query.append("SELECT DISTINCT catalog_name, schema_name AS db_schema_name")
    query.append(" FROM information_schema.schemata")

    if (catalog != null || schemaNamePattern != null) {
      query.append(" WHERE")

      var andNeeded = false
      if (catalog != null) {
        if (catalog.isEmpty()) {
          query.append(" catalog_name IS NULL")
        } else {
          query.append(" catalog_name = ?")
        }
        andNeeded = true
      }
      if (schemaNamePattern != null) {
        if (andNeeded) {
          query.append(" AND")
        }
        if (schemaNamePattern.isEmpty()) {
          query.append(" schema_name IS NULL")
        } else {
          query.append(" schema_name LIKE ?")
        }
      }
    }

    query.append(" ORDER BY catalog_name, db_schema_name;")

    val ps = database.prepare(query.toString())
    var index = 1;
    if (!catalog.isNullOrEmpty()) {
      ps.setParameter(index++, catalog)
    }
    if (!schemaNamePattern.isNullOrEmpty()) {
      ps.setParameter(index, schemaNamePattern)
    }
    return ps.closeOnQueryCompletion().executeQuery()
  }

  fun getTables(
    catalog: String?,
    schemaNamePattern: String?,
    tableNamePattern: String?,
    tableTypes: List<String>
  ): QueryResult {
    val query = StringBuilder()
    query.append("SELECT DISTINCT")
    query.append(" table_catalog AS catalog_name, table_schema AS db_schema_name, table_name, table_type")
    query.append(" FROM information_schema.tables")

    if (catalog != null || schemaNamePattern != null || tableNamePattern != null || tableTypes.isNotEmpty()) {
      query.append(" WHERE")
      var andNeeded = false

      if (catalog != null) {
        andNeeded = true
        if (catalog.isEmpty()) {
          query.append(" table_catalog IS NULL")
        } else {
          query.append(" table_catalog = ?")
        }
      }
      if (schemaNamePattern != null) {
        if (andNeeded) {
          query.append(" AND")
        }
        andNeeded = true
        if (schemaNamePattern.isEmpty()) {
          query.append(" table_schema IS NULL")
        } else {
          query.append(" table_schema LIKE ?")
        }
      }
      if (tableNamePattern != null) {
        if (andNeeded) {
          query.append(" AND")
        }
        andNeeded = true
        if (tableNamePattern.isEmpty()) {
          query.append(" table_name IS NULL")
        } else {
          query.append(" table_name LIKE ?")
        }
      }
      if (tableTypes.isNotEmpty()) {
        if (andNeeded) {
          query.append(" AND")
        }
        query.append(" table_type IN (").repeat("?, ", tableTypes.size - 1).append("?)")
      }
    }

    query.append(" ORDER BY catalog_name, db_schema_name, table_name, table_type;")

    val ps = database.prepare(query.toString())
    var index = 1;
    if (!catalog.isNullOrEmpty()) {
      ps.setParameter(index++, catalog)
    }
    if (!schemaNamePattern.isNullOrEmpty()) {
      ps.setParameter(index++, schemaNamePattern)
    }
    if (!tableNamePattern.isNullOrEmpty()) {
      ps.setParameter(index++, tableNamePattern)
    }
    for (type in tableTypes) {
      ps.setParameter(index++, type)
    }
    return ps.closeOnQueryCompletion().executeQuery()
  }

  fun addTableSchema(tablesResult: QueryResult): QueryResult {
    val tables = mutableListOf<TableDef>()
    tablesResult.consume { batch ->
      for (i in 0..<batch.rowCount) {
        tables.add(
          TableDef(
            batch.getString("catalog_name", i),
            batch.getString("db_schema_name", i),
            batch.getString("table_name", i),
            batch.getString("table_type", i)
          )
        )
      }
    }

    val newRoot = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA, allocator)
    newRoot.rowCount = tables.size
    for ((index, table) in tables.withIndex()) {
      newRoot.setString("catalog_name", index, table.catalog)
      newRoot.setString("db_schema_name", index, table.schema)
      newRoot.setString("table_name", index, table.table)
      newRoot.setString("table_type", index, table.tableType)
      newRoot.setSchema("table_schema", index, getColumns(table))
    }

    return QueryResult.forVector(newRoot)
  }

  private fun getColumns(table: TableDef): Schema {
    val fields = mutableListOf<Field>()

    database.prepare(
      """
          SELECT column_name, data_type, is_nullable
           FROM information_schema.columns
           WHERE table_catalog = ? AND table_schema = ? AND table_name = ?;
           """.trimIndent()
    )
      .closeOnQueryCompletion()
      .setParameter(1, table.catalog)
      .setParameter(2, table.schema)
      .setParameter(3, table.table)
      .executeQuery().use {
        it.consume { batch ->
          for (i in 0..<batch.rowCount) {
            val nullable = batch.getString("is_nullable", i) == "YES"
            val name = batch.getString("column_name", i)
            val type = sqlTypeToArrow(batch.getString("data_type", i))
            fields.add(
              if (nullable) Field.nullable(name, type)
              else Field.notNullable(name, type)
            )
          }
        }
      }

    return Schema(fields)
  }

  fun getTableTypes(): QueryResult =
    database.prepare("SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type;")
      .closeOnQueryCompletion()
      .executeQuery()
}

private data class TableDef(val catalog: String, val schema: String, val table: String, val tableType: String)

private fun VectorSchemaRoot.getString(fieldName: String, index: Int) =
  String((getVector(fieldName) as VarCharVector)[index], Charsets.UTF_8)

private fun VectorSchemaRoot.setString(fieldName: String, index: Int, value: String) =
  (getVector(fieldName) as VarCharVector).setSafe(index, value.toByteArray(Charsets.UTF_8))

private fun VectorSchemaRoot.setSchema(fieldName: String, index: Int, value: Schema) =
  (getVector(fieldName) as VarBinaryVector).setSafe(index, value.serializeAsMessage())
