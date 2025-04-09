package io.rzeszut.flyingducks

import com.google.protobuf.Message
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.slf4j.LoggerFactory

fun VectorSchemaRoot.getString(fieldName: String, index: Int) =
  String((getVector(fieldName) as VarCharVector)[index], Charsets.UTF_8)

fun VectorSchemaRoot.setString(fieldName: String, index: Int, value: String) =
  (getVector(fieldName) as VarCharVector).setSafe(index, value.toByteArray(Charsets.UTF_8))

fun VectorSchemaRoot.setSchema(fieldName: String, index: Int, value: Schema) =
  (getVector(fieldName) as VarBinaryVector).setSafe(index, value.serializeAsMessage())

fun VectorSchemaRoot.toParamList() =
  fieldVectors.map { it as VarCharVector }
    .map { String(it.get(0), Charsets.UTF_8) }
    .toList()

private val log = LoggerFactory.getLogger(VectorSchemaRoot::class.java)

fun VectorSchemaRoot.debugDump() {
  if (log.isDebugEnabled) {
    log.debug("Sending vector {}", contentToTSVString())
  }
}

fun Message.serialized(allocator: BufferAllocator): ArrowBuf {
  val buffer = allocator.buffer(serializedSize.toLong())
  buffer.writeBytes(toByteArray())
  return buffer
}

fun Message.serialized(allocator: BufferAllocator, block: (ArrowBuf) -> Unit) {
  allocator.buffer(serializedSize.toLong()).use { buffer ->
    buffer.writeBytes(toByteArray())
    block.invoke(buffer)
  }
}