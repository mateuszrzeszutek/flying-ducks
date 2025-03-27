package io.rzeszut.flyingducks

import com.google.protobuf.Any
import com.google.protobuf.Message
import org.apache.arrow.flight.*
import org.apache.arrow.flight.sql.FlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

class FlyingDucksServer(allocator: BufferAllocator) : FlightSqlProducer {

  // STATEMENT

  override fun getFlightInfoStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getSchemaStatement(
    command: FlightSql.CommandStatementQuery,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): SchemaResult {
    TODO("Not yet implemented")
  }

  override fun getStreamStatement(
    ticket: FlightSql.TicketStatementQuery,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // PREPARED STATEMENT

  override fun createPreparedStatement(
    request: FlightSql.ActionCreatePreparedStatementRequest,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.StreamListener<Result>
  ) {
    TODO("Not yet implemented")
  }

  override fun acceptPutPreparedStatementQuery(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: FlightProducer.CallContext,
    flightStream: FlightStream,
    listener: FlightProducer.StreamListener<PutResult>
  ): Runnable {
    TODO("Not yet implemented")
  }

  override fun getFlightInfoPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamPreparedStatement(
    command: FlightSql.CommandPreparedStatementQuery,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  override fun closePreparedStatement(
    request: FlightSql.ActionClosePreparedStatementRequest,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.StreamListener<Result>
  ) {
    TODO("Not yet implemented")
  }

  // SQL INFO

  override fun getFlightInfoSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA, descriptor, command)

  override fun getStreamSqlInfo(
    command: FlightSql.CommandGetSqlInfo,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // TYPE INFO

  override fun getFlightInfoTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo = createFlightInfo(FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, descriptor, command)

  override fun getStreamTypeInfo(
    command: FlightSql.CommandGetXdbcTypeInfo,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // CATALOGS

  override fun getFlightInfoCatalogs(
    command: FlightSql.CommandGetCatalogs,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamCatalogs(
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // SCHEMAS

  override fun getFlightInfoSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamSchemas(
    command: FlightSql.CommandGetDbSchemas,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // TABLES

  override fun getFlightInfoTables(
    command: FlightSql.CommandGetTables,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamTables(
    command: FlightSql.CommandGetTables,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // TABLE TYPES

  override fun getFlightInfoTableTypes(
    command: FlightSql.CommandGetTableTypes,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamTableTypes(
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // PRIMARY KEYS

  override fun getFlightInfoPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamPrimaryKeys(
    command: FlightSql.CommandGetPrimaryKeys,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // EXPORTED KEYS

  override fun getFlightInfoExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamExportedKeys(
    command: FlightSql.CommandGetExportedKeys,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // IMPORTED KEYS

  override fun getFlightInfoImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamImportedKeys(
    command: FlightSql.CommandGetImportedKeys,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    listener.start(VectorSchemaRoot.of())
    listener.completed()
    TODO("Not yet implemented")
  }

  // CROSS-REFERENCE

  override fun getFlightInfoCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: FlightProducer.CallContext,
    descriptor: FlightDescriptor
  ): FlightInfo {
    TODO("Not yet implemented")
  }

  override fun getStreamCrossReference(
    command: FlightSql.CommandGetCrossReference,
    callContext: FlightProducer.CallContext,
    listener: FlightProducer.ServerStreamListener
  ) {
    TODO("Not yet implemented")
  }

  // UPDATES

  override fun acceptPutStatement(
    command: FlightSql.CommandStatementUpdate,
    callContext: FlightProducer.CallContext,
    flightStream: FlightStream,
    listener: FlightProducer.StreamListener<PutResult>
  ): Runnable {
    TODO("Not yet implemented")
  }

  override fun acceptPutPreparedStatementUpdate(
    command: FlightSql.CommandPreparedStatementUpdate,
    callContext: FlightProducer.CallContext,
    flightStream: FlightStream,
    listener: FlightProducer.StreamListener<PutResult>
  ): Runnable {
    TODO("Not yet implemented")
  }

  // OTHER

  override fun listFlights(
    callContext: FlightProducer.CallContext,
    criteria: Criteria,
    listener: FlightProducer.StreamListener<FlightInfo>
  ) {
    TODO("Not yet implemented")
  }

  override fun close() {
  }

  private fun createFlightInfo(schema: Schema, descriptor: FlightDescriptor, message: Message): FlightInfo {
    val ticket = Ticket(Any.pack(message).toByteArray())
    return FlightInfo.builder(schema, descriptor, listOf(FlightEndpoint(ticket))).build()
  }
}