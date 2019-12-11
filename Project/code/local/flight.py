from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import time
# load data and record runtime
start_time = time.time()
data = pd.read_csv("../../data/airlinedelay.csv")
data.dropna(axis = 0,  inplace = True)
print("--- %s seconds ---" % (time.time() - start_time))

#data = data[["DayOfWeek","DepTime","AirTime", "ArrDelay", "DepDelay", "Distance", "CarrierDelay", "WeatherDelay", "SecurityDelay", "Cancelled"]]
#data['ArrDelay'] = np.where(data['ArrDelay'] > 0, 1, 0)
X = data[["DayOfWeek","DepTime","AirTime",  "DepDelay", "Distance", "CarrierDelay", "WeatherDelay", "SecurityDelay", "Cancelled"]]
y = data["ArrDelay"]
#y = np.where(y>0, 1, 0)
scaler = StandardScaler()
X_std = scaler.fit_transform(X)


clf = LogisticRegression(random_state=0)
start_time = time.time()
# Train model
model = clf.fit(X_std, y)
print("--- %s seconds ---" % (time.time() - start_time))

