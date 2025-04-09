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
import org.apache.arrow.vector.types.pojo.Schema
import org.slf4j.LoggerFactory

class FlyingDucksServer(private val allocator: BufferAllocator, private val database: DuckDatabase) :
  FlightSqlProducer {

  companion object {
    private val log = LoggerFactory.getLogger(FlyingDucksServer::class.java)
  }

  private val sqlInfoBuilder = SqlInfoBuilder()
    .withFlightSqlServerName("Flying Duck")
    .withFlightSqlServerVersion("0.0.1")
    .withFlightSqlServerCancel(false)
    .withFlightSqlServerSql(true)
    .withFlightSqlServerSubstrait(true)
    .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
    .withSqlCatalogAtStart(false)
    .withSqlIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
    .withSqlIdentifierQuoteChar("\"")
    .withSqlQuotedIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
    .withSqlSearchStringEscape("\\")
    .withSqlTransactionsSupported(false)

  // STATEMENT

  override fun getFlightInfoStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoStatement({})", command)

    val handle = StatementHandle(command.query)
    database.prepare(handle.sql).use { statement ->
      val schema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      val ticket = FlightSql.TicketStatementQuery.newBuilder()
        .setStatementHandle(handle.toProto())
        .build()
      createFlightInfo(schema, descriptor, ticket)
    }
  }

  override fun getSchemaStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): SchemaResult = ErrorHandler.wrap {
    log.info("Server: getSchemaStatement({})", command)

    database.prepare(command.query).use { statement ->
      val schema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      SchemaResult(schema)
    }
  }

  override fun getStreamStatement(
    ticket: FlightSql.TicketStatementQuery,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamStatement({})", ticket)

    val handle = StatementHandle.fromProto(ticket.statementHandle)
    database.prepare(handle.sql).use { statement ->
      statement.executeQuery().use { result ->
        result.send(listener)
      }
    }
  }

  override fun acceptPutStatement(
    command: FlightSql.CommandStatementUpdate,
    callContext: CallContext,
    flightStream: FlightStream,
    listener: StreamListener<PutResult>
  ): Runnable {
    log.info("Server: acceptPutStatement({})", command)

    return Runnable {
      ErrorHandler.wrap(listener) {
        val updatedCount = database.prepare(command.query).use { it.executeUpdate() }

        listener.onNext(
          PutResult.metadata(
            FlightSql.DoPutUpdateResult.newBuilder()
              .setRecordCount(updatedCount.toLong())
              .build()
              .serialized(allocator)
          )
        )
        listener.onCompleted()
      }
    }
  }

  // PREPARED STATEMENT

  override fun createPreparedStatement(
    request: FlightSql.ActionCreatePreparedStatementRequest,
    callContext: CallContext,
    listener: StreamListener<Result>
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: createPreparedStatement({})", request)

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
  ): Runnable {
    log.info("Server: acceptPutPreparedStatementQuery({})", command)

    return Runnable {
      ErrorHandler.wrap(listener) {
        val handle = PreparedStatementHandle.fromProto(command.preparedStatementHandle)

        if (!flightStream.next()) {
          if (handle.parameterCount != 0) {
            listener.onError(
              CallStatus.INTERNAL
                .withDescription("Expected ${handle.parameterCount} params, got none")
                .toRuntimeException()
            )
          } else {
            listener.onCompleted()
          }
          return@wrap
        }

        val root = flightStream.root
        if (root.rowCount != 1) {
          listener.onError(
            CallStatus.INTERNAL
              .withDescription("Only a single set of parameters was expected")
              .toRuntimeException()
          )
          return@wrap
        }

        val newHandle = handle.copy(parameterValues = root.toParamList())

        listener.onNext(
          PutResult.metadata(
            FlightSql.DoPutPreparedStatementResult.newBuilder()
              .setPreparedStatementHandle(newHandle.toProto())
              .build()
              .serialized(allocator)
          )
        )
        listener.onCompleted()
      }
    }
  }

  override fun getFlightInfoPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoPreparedStatement({})", command)

    val handle = PreparedStatementHandle.fromProto(command.preparedStatementHandle)
    database.prepare(handle.sql).use { statement ->
      val schema = JdbcToArrow.toSchema(statement.resultSetMetaData())
      val ticket = FlightSql.CommandPreparedStatementQuery.newBuilder()
        .setPreparedStatementHandle(handle.toProto())
        .build()
      createFlightInfo(schema, descriptor, ticket)
    }
  }

  override fun getStreamPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamPreparedStatement({})", command)

    val handle = PreparedStatementHandle.fromProto(command.preparedStatementHandle)
    database.prepare(handle.sql).use { statement ->
      statement
        .setParameters(handle.parameterValues)
        .executeQuery().use { result ->
          result.send(listener)
        }
    }
  }

  override fun acceptPutPreparedStatementUpdate(
    command: FlightSql.CommandPreparedStatementUpdate,
    callContext: CallContext,
    flightStream: FlightStream,
    listener: StreamListener<PutResult>
  ): Runnable {
    log.info("Server: acceptPutPreparedStatementUpdate({})", command)

    return Runnable {
      ErrorHandler.wrap(listener) {
        val handle = PreparedStatementHandle.fromProto(command.preparedStatementHandle)
        val updatedCount = database.prepare(handle.sql).use { statement ->
          while (flightStream.next()) {
            statement.setParameters(flightStream.root.toParamList()).addBatch()
          }
          statement.executeBatch()
        }.sum()

        listener.onNext(
          PutResult.metadata(
            FlightSql.DoPutUpdateResult.newBuilder()
              .setRecordCount(updatedCount.toLong())
              .build()
              .serialized(allocator)
          )
        )
        listener.onCompleted()
      }
    }
  }

  override fun closePreparedStatement(
    request: FlightSql.ActionClosePreparedStatementRequest,
    callContext: CallContext,
    listener: StreamListener<Result>
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: closePreparedStatement({})", request)

    // nothing to do, since all statements are stateless
    listener.onCompleted()
  }

  // SQL INFO

  override fun getFlightInfoSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoSqlInfo({})", command)
    createFlightInfo(Schemas.GET_SQL_INFO_SCHEMA, descriptor, command)
  }

  override fun getStreamSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamSqlInfo({})", command)
    sqlInfoBuilder.send(command.infoList, listener)
  }

  // TYPE INFO

  override fun getFlightInfoTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoTypeInfo({})", command)
    createFlightInfo(Schemas.GET_TYPE_INFO_SCHEMA, descriptor, command)
  }

  override fun getStreamTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamTypeInfo({})", command)
    QueryResult.empty(allocator, Schemas.GET_TYPE_INFO_SCHEMA).send(listener)
  }

  // CATALOGS

  override fun getFlightInfoCatalogs(
    command: FlightSql.CommandGetCatalogs,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoCatalogs({})", command)
    createFlightInfo(Schemas.GET_CATALOGS_SCHEMA, descriptor, command)
  }

  override fun getStreamCatalogs(
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamCatalogs()")
    database.metadata().getCatalogs().use { result ->
      result.send(listener)
    }
  }

  // SCHEMAS

  override fun getFlightInfoSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoSchemas({})", command)
    createFlightInfo(Schemas.GET_SCHEMAS_SCHEMA, descriptor, command)
  }

  override fun getStreamSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamSchemas({})", command)

    val catalog = if (command.hasCatalog()) command.catalog else null
    val schemaNamePattern = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null

    database.metadata().getSchemas(catalog, schemaNamePattern).use { result ->
      result.send(listener)
    }
  }

  // TABLES

  override fun getFlightInfoTables(
    command: FlightSql.CommandGetTables,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoTables({})", command)

    createFlightInfo(
      if (command.includeSchema) Schemas.GET_TABLES_SCHEMA else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
      descriptor,
      command
    )
  }

  override fun getStreamTables(
    command: FlightSql.CommandGetTables,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamTables({})", command)

    val catalog = if (command.hasCatalog()) command.catalog else null
    val schemaNamePattern = if (command.hasDbSchemaFilterPattern()) command.dbSchemaFilterPattern else null
    val tableNamePattern = if (command.hasTableNameFilterPattern()) command.tableNameFilterPattern else null
    val tableTypes: List<String> = command.tableTypesList

    database.metadata().getTables(catalog, schemaNamePattern, tableNamePattern, tableTypes).use { result ->
      if (command.includeSchema) {
        database.metadata().addTableSchema(result).use {
          it.send(listener)
        }
      } else {
        result.send(listener)
      }
    }
  }

  // TABLE TYPES

  override fun getFlightInfoTableTypes(
    command: FlightSql.CommandGetTableTypes,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoTableTypes({})", command)
    createFlightInfo(Schemas.GET_TABLE_TYPES_SCHEMA, descriptor, command)
  }

  override fun getStreamTableTypes(
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamTableTypes()")
    database.metadata().getTableTypes().use { result ->
      result.send(listener)
    }
  }

  // PRIMARY KEYS

  override fun getFlightInfoPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoPrimaryKeys({})", command)
    createFlightInfo(Schemas.GET_PRIMARY_KEYS_SCHEMA, descriptor, command)
  }

  override fun getStreamPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamPrimaryKeys({})", command)
    QueryResult.empty(allocator, Schemas.GET_PRIMARY_KEYS_SCHEMA).send(listener)
  }

  // EXPORTED KEYS

  override fun getFlightInfoExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoExportedKeys({})", command)
    createFlightInfo(Schemas.GET_EXPORTED_KEYS_SCHEMA, descriptor, command)
  }

  override fun getStreamExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamExportedKeys({})", command)
    QueryResult.empty(allocator, Schemas.GET_EXPORTED_KEYS_SCHEMA).send(listener)
  }

  // IMPORTED KEYS

  override fun getFlightInfoImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoImportedKeys({})", command)
    createFlightInfo(Schemas.GET_IMPORTED_KEYS_SCHEMA, descriptor, command)
  }

  override fun getStreamImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamImportedKeys({})", command)
    QueryResult.empty(allocator, Schemas.GET_IMPORTED_KEYS_SCHEMA).send(listener)
  }

  // CROSS-REFERENCE

  override fun getFlightInfoCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = ErrorHandler.wrap {
    log.info("Server: getFlightInfoCrossReference({})", command)
    createFlightInfo(Schemas.GET_CROSS_REFERENCE_SCHEMA, descriptor, command)
  }

  override fun getStreamCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: CallContext,
    listener: ServerStreamListener
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: getStreamCrossReference({})", command)
    QueryResult.empty(allocator, Schemas.GET_CROSS_REFERENCE_SCHEMA).send(listener)
  }

  // OTHER

  override fun listFlights(
    callContext: CallContext,
    criteria: Criteria,
    listener: StreamListener<FlightInfo>
  ) = ErrorHandler.wrap(listener) {
    log.info("Server: listFlights()")
    listener.onError(
      CallStatus.UNIMPLEMENTED
        .withDescription("Listing in-flight operations is not supported")
        .toRuntimeException()
    )
  }

  override fun close() {
  }

  private fun createFlightInfo(schema: Schema, descriptor: FlightDescriptor, message: Message): FlightInfo {
    val ticket = Ticket(Any.pack(message).toByteArray())
    return FlightInfo.builder(schema, descriptor, listOf(FlightEndpoint(ticket))).build()
  }
}