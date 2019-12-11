from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import pandas as pd 
import time
# load data and record runtime
start_time = time.time()
data = pd.read_csv("../../data/diabetes.csv")
data.dropna(axis = 0,  inplace = True)
#  how = 'any',
print("--- %s seconds ---" % (time.time() - start_time))


X = data.loc[:,:"Outcome"]
y = data["Outcome"]

scaler = StandardScaler()
X_std = scaler.fit_transform(X)


clf = LogisticRegression(random_state=0)
start_time = time.time()
# Train model
model = clf.fit(X_std, y)
print("--- %s seconds ---" % (time.time() - start_time))
