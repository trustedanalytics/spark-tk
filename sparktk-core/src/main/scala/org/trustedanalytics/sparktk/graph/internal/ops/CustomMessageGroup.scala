/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.sparktk.graph.internal.ops

import org.trustedanalytics.sparktk.frame.Frame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.{ DST, SRC, ID }
import org.apache.spark.sql.DataFrame
import org.graphframes.lib.AggregateMessages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import org.trustedanalytics.sparktk.graph.internal.{ GraphState, GraphSummarization, BaseGraph }

object GraphHelpers {
  /**
   * agg is a custom version of the aggregate system from graph frames. This method returns the
   * frame with all the appropriate messages, but does not run groupby. The use of this is to use
   * custom groupby logic (groupby on multiple columns most notably).
   *
   * Conventionally the reutrn value should have "groupby" called on it with ID as one of the grouping
   * columns
   *
   * @param g the graphframe to have messages sent
   * @param msgToSrc the message to be sent to the source vertex of each edge
   * @param msgToDst the message to be sent to the destination vertex of each edge
   * @return An ungrouped dataframe with joined messages
   */
  def agg(g: GraphFrame, msgToSrc: Option[Column], msgToDst: Option[Column]): DataFrame = {
    require(msgToSrc.nonEmpty || msgToDst.nonEmpty, s"To run GraphFrame.aggregateMessages," +
      s" messages must be sent to src, dst, or both.  Set using sendToSrc(), sendToDst().")
    val triplets = g.triplets
    val sentMsgsToSrc = msgToSrc.map { msg =>
      val msgsToSrc = triplets.select(msg.as("MSG"), triplets(SRC)(ID).as(ID))
      // Inner join: only send messages to vertices with edges
      msgsToSrc.join(g.vertices, ID)
        .select(msgsToSrc("MSG"), col(ID))
    }
    val sentMsgsToDst = msgToDst.map { msg =>
      val msgsToDst = triplets.select(msg.as("MSG"), triplets(DST)(ID).as(ID))
      msgsToDst.join(g.vertices, ID)
        .select(msgsToDst("MSG"), col(ID))
    }
    val unionedMsgs = (sentMsgsToSrc, sentMsgsToDst) match {
      case (Some(toSrc), Some(toDst)) =>
        toSrc.unionAll(toDst)
      case (Some(toSrc), None) => toSrc
      case (None, Some(toDst)) => toDst
      case _ =>
        // Should never happen. Specify this case to avoid compilation warnings.
        throw new RuntimeException("AggregateMessages: No messages were specified to be sent.")
    }
    unionedMsgs
  }
}
