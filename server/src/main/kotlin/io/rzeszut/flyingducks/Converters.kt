package io.rzeszut.flyingducks

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import java.sql.ResultSetMetaData
import java.sql.Types

object JdbcToArrow {

  fun toSchema(meta: ResultSetMetaData): Schema {
    val fields: MutableList<Field> = mutableListOf()
    for (i in 1..meta.columnCount) {
      fields.add(Field.nullable(meta.getColumnName(i), sqlTypeToArrow(meta.getColumnType(i))))
    }
    return Schema(fields)
  }

  private fun sqlTypeToArrow(sqlType: Int) = when (sqlType) {
    Types.INTEGER, Types.BIGINT -> ArrowType.Int(64, true)
    Types.FLOAT, Types.DOUBLE -> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    Types.VARCHAR -> ArrowType.Utf8()
    Types.TIMESTAMP -> ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
    else -> TODO("support other types")
  }
}