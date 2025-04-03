package io.rzeszut.flyingducks

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.apache.arrow.flight.*
import org.apache.arrow.flight.FlightProducer.*
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.SqlInfoBuilder
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.intellij.lang.annotations.Language

class FlyingDucksServer(private val allocator: BufferAllocator, private val database: DuckDatabase) :
  FlightSqlProducer {

  private val sqlInfoBuilder = SqlInfoBuilder()
    .withFlightSqlServerName("Flying Duck")
    .withFlightSqlServerVersion("0.0.1")

  // STATEMENT

  override fun getFlightInfoStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    val handle = StatementHandle(command.query)
    database.prepare(handle.sql).use { statement ->
      val schema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      val ticket = FlightSql.TicketStatementQuery.newBuilder()
        .setStatementHandle(handle.toProto())
        .build()
      return createFlightInfo(schema, descriptor, ticket)
    }
  }

  override fun getSchemaStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): SchemaResult {
    database.prepare(command.query).use { statement ->
      val schema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      return SchemaResult(schema)
    }
  }

  override fun getStreamStatement(
    ticket: FlightSql.TicketStatementQuery,
    callContext: CallContext,
    listener: ServerStreamListener
  ) {
    val handle = StatementHandle.fromProto(ticket.statementHandle)
    streamQuery(handle.sql, listener)
  }

  override fun acceptPutStatement(
    command: FlightSql.CommandStatementUpdate,
    callContext: CallContext,
    flightStream: FlightStream,
    listener: StreamListener<PutResult>
  ) = Runnable {
    TODO("Not yet implemented")
  }

  // PREPARED STATEMENT

  override fun createPreparedStatement(
    request: FlightSql.ActionCreatePreparedStatementRequest,
    callContext: CallContext,
    listener: StreamListener<Result>
  ) {
    database.prepare(request.query).use { statement ->
      val handle = PreparedStatementHandle(request.query, statement.parameterMetaData().parameterCount)
      val datasetSchema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      val parameterSchema = handle.generateParameterSchema()

      val ticket = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
        .setPreparedStatementHandle(handle.toProto())
        .setDatasetSchema(ByteString.copyFrom(datasetSchema.serializeAsMessage()))
        .setParameterSchema(ByteString.copyFrom(parameterSchema.serializeAsMessage()))
        .build()
      listener.onNext(Result(Any.pack(ticket).toByteArray()))
      listener.onCompleted()
    }
  }

  override fun acceptPutPreparedStatementQuery(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: CallContext,
    flightStream: FlightStream,
    listener: StreamListener<PutResult>
  ) = Runnable {
    TODO("Not yet implemented")
  }

  override fun getFlightInfoPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: CallContext,
    listener: ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  override fun acceptPutPreparedStatementUpdate(
    command: FlightSql.CommandPreparedStatementUpdate,
    callContext: CallContext,
    flightStream: FlightStream,
    listener: StreamListener<PutResult>
  ) = Runnable {
    TODO("Not yet implemented")
  }

  override fun closePreparedStatement(
    request: FlightSql.ActionClosePreparedStatementRequest,
    callContext: CallContext,
    listener: StreamListener<Result>
  ) {
    TODO("Not yet implemented")
  }

  // SQL INFO

  override fun getFlightInfoSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_SQL_INFO_SCHEMA, descriptor, command)

  override fun getStreamSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = sqlInfoBuilder.send(command.infoList, listener)

  // TYPE INFO

  override fun getFlightInfoTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_TYPE_INFO_SCHEMA, descriptor, command)

  override fun getStreamTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = emptyStream(Schemas.GET_TYPE_INFO_SCHEMA, listener)

  // CATALOGS

  override fun getFlightInfoCatalogs(
    command: FlightSql.CommandGetCatalogs,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_CATALOGS_SCHEMA, descriptor, command)

  override fun getStreamCatalogs(
    callContext: CallContext,
    listener: ServerStreamListener
  ) = streamQuery("select distinct catalog_name from information_schema.schemata;", listener)

  // SCHEMAS

  override fun getFlightInfoSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_SCHEMAS_SCHEMA, descriptor, command)

  override fun getStreamSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = streamQuery(
    "select distinct catalog_name, schema_name as db_schema_name from information_schema.schemata;",
    listener
  )

  // TABLES

  override fun getFlightInfoTables(
    command: FlightSql.CommandGetTables,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(
    if (command.includeSchema) Schemas.GET_TABLES_SCHEMA else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
    descriptor,
    command
  )

  override fun getStreamTables(
    command: FlightSql.CommandGetTables,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = if (command.includeSchema) streamQuery(
    """
      select distinct
             table_catalog as catalog_name,
             table_schema as db_schema_name,
             table_name,
             table_type
        from information_schema.tables;
      """.trimIndent(),
    listener
  )
  else TODO("column schema!")

  // TABLE TYPES

  override fun getFlightInfoTableTypes(
    command: FlightSql.CommandGetTableTypes,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_TABLE_TYPES_SCHEMA, descriptor, command)

  override fun getStreamTableTypes(
    callContext: CallContext,
    listener: ServerStreamListener
  ) = streamQuery("select distinct table_type from information_schema.tables;", listener)

  // PRIMARY KEYS

  override fun getFlightInfoPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_PRIMARY_KEYS_SCHEMA, descriptor, command)

  override fun getStreamPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = emptyStream(Schemas.GET_PRIMARY_KEYS_SCHEMA, listener)

  // EXPORTED KEYS

  override fun getFlightInfoExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_EXPORTED_KEYS_SCHEMA, descriptor, command)

  override fun getStreamExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = emptyStream(Schemas.GET_EXPORTED_KEYS_SCHEMA, listener)

  // IMPORTED KEYS

  override fun getFlightInfoImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_IMPORTED_KEYS_SCHEMA, descriptor, command)

  override fun getStreamImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = emptyStream(Schemas.GET_IMPORTED_KEYS_SCHEMA, listener)

  // CROSS-REFERENCE

  override fun getFlightInfoCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(Schemas.GET_CROSS_REFERENCE_SCHEMA, descriptor, command)

  override fun getStreamCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = emptyStream(Schemas.GET_CROSS_REFERENCE_SCHEMA, listener)

  // OTHER

  override fun listFlights(
    callContext: CallContext,
    criteria: Criteria,
    listener: StreamListener<FlightInfo>
  ) {
    TODO("Not yet implemented")
  }

  override fun close() {
  }

  private fun createFlightInfo(schema: Schema, descriptor: FlightDescriptor, message: Message): FlightInfo {
    val ticket = Ticket(Any.pack(message).toByteArray())
    return FlightInfo.builder(schema, descriptor, listOf(FlightEndpoint(ticket))).build()
  }

  private fun emptyStream(schema: Schema, listener: ServerStreamListener) {
    VectorSchemaRoot.create(schema, allocator).use { root ->
      listener.start(root)
      listener.putNext()
      listener.completed()
    }
  }

  private fun streamQuery(@Language("SQL") sql: String, listener: ServerStreamListener) {
    database.prepare(sql).use { statement ->
      statement.executeQuery(allocator).use { reader ->
        listener.start(reader.vectorSchemaRoot)
        while (reader.loadNextBatch()) {
          listener.putNext()
        }
        listener.completed()
      }
    }
  }
}