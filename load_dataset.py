import pandas as pd
from tqdm import tqdm
from neo4j_driver import DBHelper
import asyncio

# df = pd.read_csv('data/newDataSet.tsv', sep='\t', encoding='latin1')
# df.columns=['idx', 'uid', 'vid', 'vcid', 'vc_name', 'lat', 'long', 'utc', 'time']

# print(len(df))

# uri = "neo4j://34.94.15.179:7687"
# password = 'kKFmPWpZRHz332mE'
uri = "bolt://localhost:7687"
password = 'siso@123'
db = DBHelper(uri, password)
db.add_relationships_from_tsv(f"file:///split/loadDataSet1.tsv")
# for i in tqdm(range(200)):
#     db.add_relationships_from_tsv(f"file:///split/loadDataSet{i}.tsv")