import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

def predict_future(df, steps=10):

    data = df["revenue"]

    model = ARIMA(data, order=(2,1,2))
    model_fit = model.fit()

    forecast = model_fit.forecast(steps=steps)

    return pd.DataFrame({
        "predicted_revenue": forecast
    })