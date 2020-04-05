import json
import time
from collections import Counter
from mpi4py import MPI
import pprint

start = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


hashtags = {}
with open("smallTwitter.json","r",encoding='utf-8') as f:
    count = 0
    for line in f:
        # allocate lines to different cores respectively
        if rank == count % size:
            try:
                # get away the comma and \n at end on each line
                row = json.loads(line[:-2])
            except Exception as e1:
                try:
                    row = json.loads(line[:-1])
                except Exception as e2:
                    # to deal with the final line: ]}
                    continue
        line = row
        # doc = line['doc']
        value_hashtags = line["doc"]["entities"]["hashtags"]
        if value_hashtags:
            for a in value_hashtags:
                try:
                    hashtags[a['text']] += 1
                except:
                    hashtags[a['text']] = 1

# hashtags = sorted(hashtags)
x = sorted(hashtags.items(), key=lambda item:item[1], reverse=True)
x = x[:10]
print("THE TOP 10 POPULAR HASHTAGES are:",x)
pass