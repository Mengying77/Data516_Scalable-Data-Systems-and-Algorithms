import pandas as pd
filenames = ['airlinedelay.csv']
for i in range(10):
    filenames.append('airlinedelay.csv')
combined_csv = pd.concat( [pd.read_csv(f) for f in filenames] )
combined_csv.to_csv( "combined_csv.csv", index=False )
