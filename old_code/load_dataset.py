import pandas as pd
from tqdm import tqdm
from neo4j_driver import DBHelper
import asyncio

df = pd.read_csv('data/newDataSet.tsv', sep='\t', encoding='latin1')
df.columns=['idx', 'uid', 'vid', 'vcid', 'vc_name', 'lat', 'long', 'utc', 'time']

print(len(df))

uri = "neo4j://34.94.15.179:7687"
password = 'kKFmPWpZRHz332mE'
db = DBHelper(uri, password)
# cache_uid = {}
# cache_loc = {}
# for _, row in tqdm(df.iterrows(), total=len(df)):
#     if row['uid'] not in cache_uid:
#         if db.user_exists(row['uid']) is None:
#             db.create_user(row['uid'])
#             cache_uid[row['uid']] = True
#     if row['vid'] not in cache_loc:
#         if db.location_exists(row['vid']) is None:
#             db.create_location(row['vid'])
#             cache_loc[row['vid']] = True
#     #print(row['time'], type(row['time']))
#     db.add_relationship(row['uid'], row['vid'], row['time'])
async def add_all_users(users):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            db.create_user,
            user
        )
        for user in users
    ]
    [await f for f in tqdm(asyncio.as_completed(futures), total=len(users))]

async def add_all_locations(locs):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            db.create_location,
            loc
        )
        for loc in locs
    ]
    [await f for f in tqdm(asyncio.as_completed(futures), total=len(locs))]

async def add_all_rel(df):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            db.add_relationship,
            row['uid'], row['vid'], row['time']
        )
        for _, row in df.iterrows()
    ]
    [await f for f in tqdm(asyncio.as_completed(futures), total=len(df))]

if __name__ == "__main__":
    per_iter = 50000
    max_iter = int(len(df)/per_iter)
    users = list(df['uid'].unique())
    locs = list(df['vid'].unique())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_all_users(users))
    loop.run_until_complete(add_all_locations(locs))
    for i in tqdm(range(int(len(df)/per_iter)+1)):
        if i == max_iter:
            loop.run_until_complete(add_all_rel(df.iloc[i*per_iter:,:]))
        else:
            loop.run_until_complete(add_all_rel(df.iloc[i*per_iter:(i+1)*per_iter,:]))