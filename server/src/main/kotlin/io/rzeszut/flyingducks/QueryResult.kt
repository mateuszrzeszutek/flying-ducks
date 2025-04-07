package io.rzeszut.flyingducks

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

sealed interface QueryResult : AutoCloseable {

  companion object {
    fun empty(allocator: BufferAllocator, schema: Schema): QueryResult = EmptyResult(allocator, schema)

    fun forReader(reader: ArrowReader): QueryResult = ArrowReaderResult(reader)

    fun forVector(vectorSchemaRoot: VectorSchemaRoot): QueryResult = VectorResult(vectorSchemaRoot)
  }

  fun send(listener: ServerStreamListener)

  fun consume(consumer: (VectorSchemaRoot) -> Unit)
}

private data class EmptyResult(val allocator: BufferAllocator, val schema: Schema) : QueryResult {

  override fun send(listener: ServerStreamListener) {
    VectorSchemaRoot.create(schema, allocator).use { root ->
      listener.start(root)
      listener.putNext()
      listener.completed()
    }
  }

  override fun consume(consumer: (VectorSchemaRoot) -> Unit) {}

  override fun close() {
  }
}

private data class ArrowReaderResult(val reader: ArrowReader) : QueryResult {

  override fun send(listener: ServerStreamListener) {
    listener.start(reader.vectorSchemaRoot)
    while (reader.loadNextBatch()) {
      reader.vectorSchemaRoot.debugDump()
      listener.putNext()
    }
    listener.completed()
  }

  override fun consume(consumer: (VectorSchemaRoot) -> Unit) {
    while (reader.loadNextBatch()) {
      consumer.invoke(reader.vectorSchemaRoot)
    }
  }

  override fun close() {
    reader.close()
  }
}

private data class VectorResult(val root: VectorSchemaRoot) : QueryResult {

  override fun send(listener: ServerStreamListener) {
    listener.start(root)
    root.debugDump()
    listener.putNext()
    listener.completed()
  }

  override fun consume(consumer: (VectorSchemaRoot) -> Unit) {
    consumer.invoke(root)
  }

  override fun close() {
    root.close()
  }
}
