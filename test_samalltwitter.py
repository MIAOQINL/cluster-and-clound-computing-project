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
language = {}
dict_language = {'aa':'Afar','sq': 'Albanian','am': 'Amharic','ar': 'Arabic','hy': 'Armenian','eu': 'Basque',
                 'bn': 'Bengali','bg': 'Bulgarian','my': 'Burmese','cs': 'Czech','zh': 'Chinese','kw': 'Cornish',
                 'cs': 'Czech','da': 'Danish','de': 'German','dv': 'Divehi; Dhivehi; Maldivian',
                 'nl': 'Dutch Flemish','el': 'Greek, Modern (1453-)','en': 'English','fa': 'Persian','fj': 'Fijian',
                 'fi': 'Finnish','fr': 'French','Ga': 'Georgian','de': 'German','gd': 'Gaelic; Scottish Gaelic',
                 'ga': 'Irish','gl': 'Galician','gv': 'Manx','el': 'Greek, Modern (1453-)','he': 'Hebrew',
                 'hi': 'Hindi','ho': 'Hiri Motu','hu': 'Hungarian','hy': 'Armenian','id': 'Indonesian',
                 'in':'in','it': 'Italian','ja': 'Japanese','ko': 'Korean','msa': 'Malay','nl': 'Dutch; Flemish','no': 'Norwegian',
                 'fa': 'Persian','pl': 'Polish','pt': 'Portuguese','ro': 'Romanian','rn': 'Rundi','ru': 'Russian',
                 'es': 'Spanish; Castilian','sv': 'Swedish','th': 'Thai','tl': 'Tagalog','tr': 'Turkish','uk': 'Ukrainian',
                 'ur': 'Urdu','und':'Undetermined','vi': 'Vietnamese','vo': 'Volapük','zh': 'Chinese'}

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
        value_language = line["doc"]["metadata"]["iso_language_code"]
        if value_hashtags:
            for a in value_hashtags:
                try:
                    hashtags[a['text']] += 1
                except:
                    hashtags[a['text']] = 1
        if value_language:
            try:
                language[value_language] += 1
            except:
                language[value_language] = 1


# top 10 of hashtags
hashtags_count = sorted(hashtags.items(), key=lambda item:item[1], reverse=True)
hashtags_count = hashtags_count[:10]

# top 10 of language
language_count = sorted(language.items(), key=lambda item:item[1], reverse=True)
language_count = language_count[:10]

for i in range(len(language_count)):
    language_count[i] = list(language_count[i])
    language_count[i][0] = dict_language[language_count[i][0]]

end = time.time()
print()
print('TIME CONSUMPTION: {} in seconds'.format(end - start))

print("THE TOP 10 POPULAR HASHTAGES are:",hashtags_count)
print("THE TOP 10 POPULAR language are:",language_count)




pass