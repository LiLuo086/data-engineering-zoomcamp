import sys
import pandas as pd

print("arguments:", sys.argv)

month = int(sys.argv[1])

df=pd.DataFrame({"day":[1,2],
                 "num_passengers":[100,150]})

print(df.head())

df.to_parquet(f'output_data_{month:02d}.parquet')

print(f'Processing data for month: {month}')