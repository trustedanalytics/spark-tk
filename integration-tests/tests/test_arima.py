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

