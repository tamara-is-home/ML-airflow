import pickle
import pandas as pd


def predict(data: pd.DataFrame):
    model = pickle.load(open("rf_final_model_daily.sav", 'rb'))
    data = data.resample('d').sum()
    preds = model.predict(data)
    print('predictions ready!')
    return preds[0]
