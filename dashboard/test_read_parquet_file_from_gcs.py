import pandas as pd
import pyarrow.parquet as pq

from pyarrow import fs
# Read parquet data from GCS - https://arrow.apache.org/docs/python/filesystems.html#filesystem-gcs

URI = "data-lake-accidents/data/accidents_gold_agg.parquet"

# Read file from GCS using pandas
gcs = fs.GcsFileSystem(anonymous=True)
df = pq.ParquetDataset(URI, filesystem=gcs)#.read_pandas().to_pandas()