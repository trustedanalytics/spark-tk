# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from setup import tc, rm, get_sandbox_path

import logging
logger = logging.getLogger(__name__)
import os

def test_arima_save_load(tc):
    ts = [12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595]
    save_path = "sandbox/arima_save_test"
    original_model = tc.models.timeseries.arima.train(ts, 1, 0, 1)
    original_predict = original_model.predict(0)
    # Save model
    original_model.save(save_path)

    # Load the model and check that it's the same type as the saved model
    loaded_model = tc.load(save_path)
    assert(type(original_model) == type(loaded_model))
    assert(original_model.p == loaded_model.p)
    assert(original_model.d == loaded_model.d)
    assert(original_model.q == loaded_model.q)
    assert(original_model.include_intercept == loaded_model.include_intercept)
    assert(original_model.init_params == loaded_model.init_params)
    assert(original_model.coefficients == loaded_model.coefficients)
    assert(ts == loaded_model.ts_values)
    assert(original_predict == loaded_model.predict(0))

def test_arima_predict(tc):
    ts = [12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595]
    model = tc.models.timeseries.arima.train(ts, 1, 0, 1)

    # predict using the same values that were used with training
    predicted_vals = model.predict(0)

    # optionally, provide a list of ts values to use as the gold standard
    predicted_with_ts = model.predict(0, ts)

    # the predicted values should be the same, since the same ts was used for both training and predict
    assert(predicted_vals == predicted_with_ts)

    # use different golden values and verify that the predicts are different
    predicted_diff_ts = model.predict(0, [15.2, 12.7, 14.4, 15.7, 18.0, 13.11])
    assert(predicted_diff_ts != predicted_vals)