import os
import pandas as pd 
file_path = '/data1/ercot_data/load_data'
files = os.listdir(file_path)
dfs= [pd.read_excel(os.path.join(file_path,i)) for i in files]