"""from utils.bq_data_extraction import get_all_data_for_date
from utils.data_transformation import transform
from utils.model_evaluation import predict
import datetime
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "sound-potion-312013-6ddcdd6d2c36.json"


def combine_all ( ):
    yesterday = str (datetime.date.today ()-datetime.timedelta (days = 1))
    today = str (datetime.date.today ())
    data_raw = get_all_data_for_date (yesterday, today)
    data_transformed = transform (data_raw, yesterday, today)
    prediction = predict (data_transformed)
    print(prediction)

combine_all()"""