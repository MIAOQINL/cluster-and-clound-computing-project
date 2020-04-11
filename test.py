import json
import time
from mpi4py import MPI
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

read_json = []
count = 0
hashtags = {}
language = {}

start = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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

with open('bigTwitter.json', 'r', encoding='utf-8') as f:
    for lines in f:
        lines = lines.strip(',\n')
        if rank == count % size:
            try:
                line = json.loads(lines)
            except Exception as e1:
                count += 1
                continue

            hashtag = line['doc']['entities']['hashtags']
            value_language = line["doc"]["metadata"]["iso_language_code"]
            if hashtag:
                for h in hashtag:
                    hashtag_text = h['text']
                    if hashtag_text in hashtags:
                        hashtags[hashtag_text] += 1
                    else:
                        hashtags[hashtag_text] = 1
            if value_language:
                try:
                    language[value_language] += 1
                except:
                    language[value_language] = 1
            count += 1
        else:
            count += 1

# print(sorted_hashtags[:10])
if rank == 0:
    for _ in range(1, size):
        received_hashtags = (comm.recv(source=_, tag = 0))
        for h in received_hashtags:
            if h in hashtags:
                hashtags[h] += received_hashtags[h]
            else:
                hashtags[h] = received_hashtags[h]
    sorted_hashtags = sorted(hashtags.items(), key=lambda item: item[1], reverse=True)
else:
    comm.send(hashtags, dest=0, tag=0)

if rank == 0:
    for _ in range(1, size):
        received_language_dic = (comm.recv(source=_, tag = 0))
        for l in received_language_dic:
            if l in language:
                language[l] += received_language_dic[l]
            else:
                hashtags[l] = received_language_dic[l]
    sorted_languages = sorted(language.items(), key=lambda item: item[1], reverse=True)
else:
    comm.send(language, dest=0, tag=0)
# top 10 of hashtags
hashtags_count = sorted_hashtags[:10]

# top 10 of language
language_count = sorted_languages[:10]

for i in range(len(language_count)):
    language_count[i] = list(language_count[i])
    language_count[i][0] = dict_language[language_count[i][0]]

end = time.time()
print()
print('TIME CONSUMPTION: {} in seconds'.format(end - start))

print("THE TOP 10 POPULAR HASHTAGES are:",hashtags_count)
print("THE TOP 10 POPULAR language are:",language_count)