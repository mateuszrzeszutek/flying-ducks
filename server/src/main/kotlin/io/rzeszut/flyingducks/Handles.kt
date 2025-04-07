package io.rzeszut.flyingducks

import com.google.protobuf.ByteString
import io.rzeszut.flyingducks.proto.PreparedStatementHandleProto
import io.rzeszut.flyingducks.proto.StatementHandleProto
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

data class StatementHandle(val sql: String) {

  fun toProto(): ByteString = StatementHandleProto.newBuilder()
    .setSql(sql)
    .build()
    .toByteString()

  companion object {
    fun fromProto(proto: ByteString) = StatementHandle(StatementHandleProto.parseFrom(proto).sql)
  }
}

data class PreparedStatementHandle(
  val sql: String,
  val parameterCount: Int,
  val parameterValues: List<String> = listOf()
) {

  fun toProto(): ByteString = PreparedStatementHandleProto.newBuilder()
    .setSql(sql)
    .setParamCount(parameterCount)
    .addAllParamValue(parameterValues)
    .build()
    .toByteString()

  fun generateParameterSchema(): Schema {
    // all params are strings, why not
    val fields = List(parameterCount) { Field.notNullable(null, ArrowType.Utf8()) }
    return Schema(fields)
  }

  companion object {
    fun fromProto(proto: ByteString): PreparedStatementHandle {
      val parsedProto = PreparedStatementHandleProto.parseFrom(proto)
      return PreparedStatementHandle(parsedProto.sql, parsedProto.paramCount, parsedProto.paramValueList)
    }
  }
}
