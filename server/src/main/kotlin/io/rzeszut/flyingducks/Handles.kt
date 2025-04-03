package io.rzeszut.flyingducks

import com.google.protobuf.ByteString
import io.rzeszut.flyingducks.proto.PreparedStatementHandleProto
import io.rzeszut.flyingducks.proto.StatementHandleProto
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

class StatementHandle(val sql: String) {

  fun toProto(): ByteString = StatementHandleProto.newBuilder()
    .setSql(sql)
    .build()
    .toByteString()

  companion object {
    fun fromProto(proto: ByteString) = StatementHandle(StatementHandleProto.parseFrom(proto).sql)
  }
}

class PreparedStatementHandle(val sql: String, val parameterCount: Int) {

  fun toProto(): ByteString = PreparedStatementHandleProto.newBuilder()
    .setSql(sql)
    .setParamCount(parameterCount)
    .build()
    .toByteString()

  fun generateParameterSchema(): Schema {
    val fields = List(parameterCount) { Field.notNullable(null, ArrowType.Null()) }
    return Schema(fields)
  }

  companion object {
    fun fromProto(proto: ByteString): PreparedStatementHandle {
      val parsedProto = PreparedStatementHandleProto.parseFrom(proto)
      return PreparedStatementHandle(parsedProto.sql, parsedProto.paramCount)
    }
  }
}
