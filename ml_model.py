import pandas as pd
import pickle
from sklearn.linear_model import LinearRegression

MODEL_PATH = "model.pkl"

# ==============================
# 🚀 TRAIN MODEL
# ==============================
def train_model(df):
    df = df.copy()

    # Create time index
    df["time_index"] = range(len(df))

    X = df[["time_index"]]
    y = df["revenue"]

    model = LinearRegression()
    model.fit(X, y)

    # Save model
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    return model


# ==============================
# 📈 LOAD MODEL
# ==============================
def load_model():
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)


# ==============================
# 🔮 PREDICT FUTURE
# ==============================
def predict_future(df, steps=10):
    try:
        model = load_model()
    except:
        model = train_model(df)

    last_index = len(df)

    future_index = list(range(last_index, last_index + steps))
    future_df = pd.DataFrame({"time_index": future_index})

    predictions = model.predict(future_df)

    future_df["predicted_revenue"] = predictions

    return future_df