package org.trustedanalytics.sparktk.frame.internal.ops.exportdata

import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, HBaseConfiguration }
import org.apache.hadoop.hbase.client.{ Put, HBaseAdmin }
import org.trustedanalytics.sparktk.frame.{ Schema, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ExportToHbaseSummarization extends BaseFrame {

  /**
   * Write current frame to HBase table.
   *
   *
   * @param tableName     The name of the HBase table that will contain the exported frame
   * @param keyColumnName The name of the column to be used as row key in hbase table
   * @param familyName    The family name of the HBase table that will contain the exported frame
   */
  def exportToHbase(tableName: String, keyColumnName: Option[String] = None, familyName: String = "family") = {
    execute(ExportToHbase(tableName, keyColumnName, familyName))
  }
}

case class ExportToHbase(tableName: String, keyColumnName: Option[String], familyName: String) extends FrameSummarization[Unit] {

  require(StringUtils.isNotEmpty(tableName), "Hbase table name is required")
  require(keyColumnName !=null, "Hbase key column name cannot be null")
  require(StringUtils.isNotEmpty(familyName), "Hbase table family name is required")

  override def work(state: FrameState): Unit = {
    ExportToHbase.exportToHbaseTable(state, tableName, keyColumnName, familyName)
  }
}

object ExportToHbase {

  def exportToHbaseTable(frameRdd: FrameRdd,
                         tableName: String,
                         keyColumnName: Option[String],
                         familyName: String) = {

    val conf = createConfig(tableName)
    val pairRdd = convertToPairRDD(frameRdd,
      familyName,
      keyColumnName.getOrElse(StringUtils.EMPTY))

    val hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create())
    if (!hBaseAdmin.tableExists(tableName)) {
      val desc = new HTableDescriptor(tableName)
      desc.addFamily(new HColumnDescriptor(familyName))

      hBaseAdmin.createTable(desc)
    }

    pairRdd.saveAsNewAPIHadoopDataset(conf)
  }

  /**
   * Creates pair rdd to save to hbase
   *
   * @param rdd              initial frame rdd
   * @param familyColumnName family column name for hbase
   * @param keyColumnName    key column name for hbase
   * @return pair rdd
   */
  def convertToPairRDD(rdd: FrameRdd,
                       familyColumnName: String,
                       keyColumnName: String) = {

    rdd.mapRows(_.valuesAsArray()).zipWithUniqueId().map {
      case (row, index) => buildRow((row, index), rdd.frameSchema, familyColumnName, keyColumnName)
    }
  }

  /**
   * Create initial configuration for hbase writer
   *
   * @param tableName name of hBase table
   * @return hBase configuration
   */
  private def createConfig(tableName: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = new Job(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.getConfiguration
  }

  /**
   * Builds a row
   *
   * @param row              row of the original frame
   * @param schema           original schema
   * @param familyColumnName family column name for hbase
   * @param keyColumnName    key column name for hbase
   * @return hbase row
   */
  private def buildRow(row: (Array[Any], Long), schema: Schema, familyColumnName: String, keyColumnName: String) = {
    val columnTypes = schema.columns.map(_.dataType)
    val columnNames = schema.columns.map(_.name)
    val familyColumnAsByteArray = Bytes.toBytes(familyColumnName)

    val valuesAsDataTypes = DataTypes.parseMany(columnTypes.toArray)(row._1)
    val valuesAsByteArray = valuesAsDataTypes.map(value => {
      if (null == value) null else Bytes.toBytes(value.toString)
    })

    val keyColumnValue = Bytes.toBytes(keyColumnName + row._2)
    val put = new Put(keyColumnValue)
    for (index <- 0 to valuesAsByteArray.length - 1) {
      if (valuesAsByteArray(index) != null) {
        put.add(familyColumnAsByteArray, Bytes.toBytes(columnNames(index)), valuesAsByteArray(index))
      }
    }

    (new ImmutableBytesWritable(keyColumnValue), put)
  }
}

