package org.trustedanalytics.sparktk.frame

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.internal.BaseFrame
import org.trustedanalytics.sparktk.frame.internal.ops._
import org.trustedanalytics.sparktk.frame.internal.ops.binning.{ BinColumnTransformWithResult, HistogramSummarization, QuantileBinColumnTransformWithResult }
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ BinaryClassificationMetricsSummarization, MultiClassClassificationMetricsSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist.{ CumulativePercentTransform, CumulativeSumTransform, EcdfSummarization, TallyPercentTransform, TallyTransform }
import org.trustedanalytics.sparktk.frame.internal.ops.join.{ JoinInnerSummarization, JoinLeftSummarization, JoinOuterSummarization, JoinRightSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.sample.AssignSampleTransform
import org.trustedanalytics.sparktk.frame.internal.ops.exportdata.{ ExportToCsvSummarization, ExportToHbaseSummarization, ExportToHiveSummarization, ExportToJdbcSummarization, ExportToJsonSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.flatten.FlattenColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.ops.RenameColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.ops.sortedk.SortedKSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation.{ CorrelationMatrixSummarization, CorrelationSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance.{ CovarianceMatrixSummarization, CovarianceSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives.{ CategoricalSummarySummarization, ColumnMedianSummarization, ColumnModeSummarization, ColumnSummaryStatisticsSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.quantiles.QuantilesSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.timeseries.{ TimeSeriesFromObseravationsSummarization, TimeSeriesSliceSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.topk.TopKSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.unflatten.UnflattenColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.ops.zip.ZipWithIndexedRddTransform
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, PythonJavaRdd }
import org.trustedanalytics.sparktk.saveload.TkSaveableObject

class Frame(frameRdd: RDD[Row], frameSchema: Schema) extends BaseFrame // params named "frameRdd" and "frameSchema" because naming them "rdd" and "schema" masks the base members "rdd" and "schema" in this scope
    with AddColumnsTransform
    with AppendFrameTransform
    with AssignSampleTransform
    with BinColumnTransformWithResult
    with BinaryClassificationMetricsSummarization
    with CategoricalSummarySummarization
    with ColumnMedianSummarization
    with ColumnModeSummarization
    with ColumnSummaryStatisticsSummarization
    with CopySummarization
    with CorrelationMatrixSummarization
    with CorrelationSummarization
    with CountSummarization
    with CovarianceMatrixSummarization
    with CovarianceSummarization
    with CumulativePercentTransform
    with CumulativeSumTransform
    with DotProductTransform
    with DropColumnsTransform
    with DropDuplicatesTransform
    with EcdfSummarization
    with EntropySummarization
    with ExportToCsvSummarization
    with ExportToHbaseSummarization
    with ExportToHiveSummarization
    with ExportToJdbcSummarization
    with ExportToJsonSummarization
    with FlattenColumnsTransform
    with HistogramSummarization
    with JoinInnerSummarization
    with JoinLeftSummarization
    with JoinOuterSummarization
    with JoinRightSummarization
    with MultiClassClassificationMetricsSummarization
    with QuantilesSummarization
    with QuantileBinColumnTransformWithResult
    with RenameColumnsTransform
    with RowCountSummarization
    with SaveSummarization
    with SortTransform
    with SortedKSummarization
    with TakeSummarization
    with TallyPercentTransform
    with TallyTransform
    with TimeSeriesFromObseravationsSummarization
    with TimeSeriesSliceSummarization
    with TopKSummarization
    with UnflattenColumnsTransform
    with ZipWithIndexedRddTransform {
  init(frameRdd, frameSchema)

  /**
   * (typically called from pyspark, with jrdd)
   *
   * @param jrdd java array of Any
   * @param schema frame schema
   */
  def this(jrdd: JavaRDD[Array[Any]], schema: Schema) = {
    this(PythonJavaRdd.toRowRdd(jrdd.rdd, schema), schema)
  }
}

object Frame extends TkSaveableObject {

  val tkFormatVersion = 1

  /**
   * Loads a parquet file found at the given path and returns a Frame
   *
   * @param sc active SparkContext
   * @param path path to the file
   * @param formatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def load(sc: SparkContext, path: String, formatVersion: Int = tkFormatVersion, tkMetadata: JValue = null): Any = {
    require(tkFormatVersion == formatVersion, s"Frame load only supports version $tkFormatVersion.  Got version $formatVersion")
    // no extra metadata in version 1
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    val frameRdd = FrameRdd.toFrameRdd(df)
    new Frame(frameRdd, frameRdd.frameSchema)
  }
}
