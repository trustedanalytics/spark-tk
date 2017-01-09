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
package org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model

import org.apache.spark.mllib.tree.configuration.FeatureType._

/**
 * Used for "binning" the feature values for faster best split calculation.
 *
 * For a continuous feature, the bin is determined by a low and a high split,
 *  where an example with featureValue falls into the bin s.t.
 *  lowSplit.threshold < featureValue <= highSplit.threshold.
 *
 * For ordered categorical features, there is a 1-1-1 correspondence between
 *  bins, splits, and feature values.  The bin is determined by category/feature value.
 *  However, the bins are not necessarily ordered by feature value;
 *  they are ordered using impurity.
 *
 * For unordered categorical features, there is a 1-1 correspondence between bins, splits,
 *  where bins and splits correspond to subsets of feature values (in highSplit.categories).
 *  An unordered feature with k categories uses (1 << k - 1) - 1 bins, corresponding to all
 *  partitionings of categories into 2 disjoint, non-empty sets.
 *
 * @param lowSplit signifying the lower threshold for the continuous feature to be
 *                 accepted in the bin
 * @param highSplit signifying the upper threshold for the continuous feature to be
 *                  accepted in the bin
 * @param featureType type of feature -- categorical or continuous
 * @param category categorical label value accepted in the bin for ordered features
 */
private[tree] case class Bin(lowSplit: Split, highSplit: Split, featureType: FeatureType, category: Double)
