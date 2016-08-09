package org.trustedanalytics.sparktk.frame.internal.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object MiscFrameFunctions extends Serializable {

  /**
   * take an input RDD and return another RDD which contains the subset of the original contents
   * @param rdd input RDD
   * @param offset rows to be skipped before including rows in the new RDD
   * @param count total rows to be included in the new RDD
   * @param limit limit on number of rows to be included in the new RDD
   */
  def getPagedRdd[T: ClassTag](rdd: RDD[T], offset: Long, count: Long, limit: Int): RDD[T] = {

    val sumsAndCounts = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(rdd)
    val capped = limit match {
      case -1 => count
      case _ => Math.min(count, limit)
    }
    //Start getting rows. We use the sums and counts to figure out which
    //partitions we need to read from and which to just ignore
    val pagedRdd: RDD[T] = rdd.mapPartitionsWithIndex((i, rows) => {
      val (ct: Long, sum: Long) = sumsAndCounts(i)
      val thisPartStart = sum - ct
      if (sum < offset || thisPartStart >= offset + capped) {
        //println("skipping partition " + i)
        Iterator.empty
      }
      else {
        val start = Math.max(offset - thisPartStart, 0)
        val numToTake = Math.min((capped + offset) - thisPartStart, ct) - start
        //println(s"partition $i: starting at $start and taking $numToTake")
        rows.slice(start.toInt, start.toInt + numToTake.toInt)
      }
    })

    pagedRdd
  }

  /**
   * take input RDD and return the subset of the original content
   * @param rdd input RDD
   * @param offset  rows to be skipped before including rows in the result
   * @param count total rows to be included in the result
   * @param limit limit on number of rows to be included in the result
   */
  def getRows[T: ClassTag](rdd: RDD[T], offset: Long, count: Int, limit: Int): Seq[T] = {
    val pagedRdd = getPagedRdd(rdd, offset, count, limit)
    val rows: Seq[T] = pagedRdd.collect()
    rows
  }

  /**
   * Return the count and accumulated sum of rows in each partition
   */
  def getPerPartitionCountAndAccumulatedSum[T](rdd: RDD[T]): Map[Int, (Long, Long)] = {
    //Count the rows in each partition, then order the counts by partition number
    val counts = rdd.mapPartitionsWithIndex(
      (i: Int, rows: Iterator[T]) => Iterator.single((i, rows.size.toLong)))
      .collect()
      .sortBy(_._1)

    //Create cumulative sums of row counts by partition, e.g. 1 -> 200, 2-> 400, 3-> 412
    //if there were 412 rows divided into two 200 row partitions and one 12 row partition
    val sums = counts.scanLeft((0L, 0L)) {
      (t1, t2) => (t2._1, t1._2 + t2._2)
    }
      .drop(1) //first one is (0,0), drop that
      .toMap

    //Put the per-partition counts and cumulative counts together
    val sumsAndCounts = counts.map {
      case (part, count) => (part, (count, sums(part)))
    }.toMap
    sumsAndCounts
  }

  /**
   * Remove duplicate rows identified by the key
   * @param pairRdd rdd which has (key, value) structure in each row
   */
  def removeDuplicatesByKey(pairRdd: RDD[(List[Any], Row)]): RDD[Row] = {
    pairRdd.reduceByKey((x, y) => x).map(x => x._2)
  }

}
