package org.trustedanalytics.sparktk.graph.internal.ops.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import org.apache.spark.sql.types._

/**
 * converts Spark data types to OrientDB data type
 */
object DataTypesConverter {

  /**
   * converts data types to OrientDB data types
   *
   * @param dataType Spark data types
   * @return OrientDB data type
   */
  def sparkToOrientdb(dataType: Any): OType = dataType match {
    case StringType => OType.STRING
    case IntegerType => OType.INTEGER
    case LongType => OType.LONG
    case FloatType => OType.FLOAT
    case ByteType => OType.BYTE
    case DoubleType => OType.DOUBLE
    case ShortType => OType.SHORT
    case BinaryType => OType.BINARY
    case BooleanType => OType.BOOLEAN
    case DateType => OType.DATE
    case TimestampType => OType.ANY
    case MapType => OType.ANY
    case StructType => OType.ANY
    case DecimalType => OType.DECIMAL
    case ArrayType => OType.ANY
    case _ => throw new IllegalArgumentException(s"Unable to convert ${dataType} to OrientDB data type")
  }

  /**
   * converts OrientDB data type to Spark data type
   *
   * @param orientDbType OrientDB data type
   * @return Spark data type
   */
  def orientdbToSpark(orientDbType: OType): DataType = orientDbType match {
    case OType.LONG => DataTypes.LongType
    case OType.INTEGER => DataTypes.IntegerType
    case OType.FLOAT => DataTypes.FloatType
    case OType.STRING => DataTypes.StringType
    case OType.BYTE => DataTypes.ByteType
    case OType.BINARY => DataTypes.BinaryType
    case OType.DOUBLE => DataTypes.DoubleType
    case OType.SHORT => DataTypes.ShortType
    case OType.BOOLEAN => DataTypes.BooleanType
    case OType.DATE => DataTypes.DateType
    case _ => throw new IllegalArgumentException(s"Unable to convert $orientDbType to Spark data type")
  }
}
