import dask.dataframe as dd

df = dd.read_csv('your_file.csv')
df.head()  # only loads a sample, not full file