import json
import nltk
import psycopg2
import csv
import string
import time



from sklearn.feature_extraction.text import TfidfVectorizer

from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import pymorphy2


def connectToDB():
    global cursor
    global connection
    with open(r'cityData/city_regions.json', encoding='utf8') as f:
        city_BD = json.load(f)

    connection = psycopg2.connect(user="postgres", database="Coursework",
                                    # пароль, который указали при установке PostgreSQL
                                    password="Samsung01",
                                    host="faff0868e784",
                                    port="5432")

    print("Connected successfuly!")
    cursor = connection.cursor()
    return connection

def ispunct(ch):
    return ch in string.punctuation

def exclude(s):
    return len(s) < 4 or ispunct(s[0]) or s[0].isdigit() or s in s_ru or s in s_en or s[:2] == "id"

def countAll(text_in,text_out,region):
    print()
    dict_in={}
    dict_out={}
    text_amount_in=len(text_in)
    text_amount_out=len(text_out)
    print("Dict in..." + str(len(text_in)))
    for text in text_in:
        text=text.split(' ')
        for word in text:
            if (word in names):
                continue
            if (word in dict_in):
                dict_in[word]+=1
            else:
                dict_in[word]=1

    for key,value in dict_in.items():
        dict_in[key]/=text_amount_in

    dict_in = list(sorted(dict_in.items(), key=lambda x: -x[1]))
    top_5000 = dict_in[:5000]

    totalList = [["Word", "Value"]]
    for vals in top_5000:
        list2 = []
        if (vals[1] == 0):
            break
        list2.append(vals[0])
        list2.append(vals[1])
        totalList.append(list2)
    with open(r"./outputData/RES_IN_" + region[0] + ".csv",
              "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(totalList)

    print("Dict out...")

    for text in text_out:
        text=text.split(' ')
        for word in text:
            if (word in names):
                continue
            if (word in dict_out):
                dict_out[word]+=1
            else:
                dict_out[word]=1

    for key,value in dict_out.items():
        dict_out[key]/=text_amount_out

    dict_out = list(sorted(dict_out.items(), key=lambda x: -x[1]))

    top_5000 = dict_out[:5000]

    totalList = [["Word", "Value"]]
    for vals in top_5000:
        list2 = []
        if (vals[1] == 0):
            break
        list2.append(vals[0])
        list2.append(vals[1])
        totalList.append(list2)
    with open(r"./outputData/RES_OUT_" + region[0] + ".csv",
              "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(totalList)

    #with open(r"C:\Users\jdczy\Desktop\Diploma\Coursework\web_app\Analys\FileStorage\RES_IN_" + region[0] + ".csv", "w", newline="", encoding="utf-8") as f:
    #    writer = csv.writer(f)
    #    writer.writerows(totalList)



    print("DONE REGION!")

morph = pymorphy2.MorphAnalyzer()

nltk.download('stopwords')
names=set()
s_ru = set(stopwords.words('russian'))
s_en = set(stopwords.words('english'))
tt = TweetTokenizer()

with open('names.jsonl',encoding='utf8') as f:
    names_l = list(f)

    for json_str in names_l:
        result = json.loads(json_str)
        names.add((result.get('text').lower()))
    print()



with open('./cityData/cities_ru_en.csv', mode='r', encoding="utf-8") as inp:
    reader = csv.reader(inp)
    eng_ru_city_dict = {rows[1]: rows[0] for rows in reader}

city_communications = {}

region_communications_pos = {}
region_communications_neg = {}
corpus_set = []
region2_text = {}

regions_amnt = {}
regions_scalar_sentiment = {}
get_first_city = "SELECT T2.title as src_city,T1.title as dst_city,T1.id,T1.id_from,T1.text FROM " \
                 "(SELECT object_comments.id, object_comments.id_from, title,text  " \
                 "FROM object_comments ,user_data_table,city " \
                 "WHERE object_comments.id=user_data_table.id AND user_data_table.city_id IS NOT NULL " \
                 "AND user_data_table.city_id=city.id_city LIMIT 100000) AS t1 INNER JOIN " \
                 "(SELECT DISTINCT id,title FROM user_data_table,city WHERE city_id=id_city ) AS t2 ON t2.id=t1.id_from"

connection=connectToDB()
cursor = connection.cursor()
cursor.execute(get_first_city)
counter = 0
with open(r'./cityData/city_regions.json', encoding='utf8') as f:
    geoRegions = json.load(f)

for row2 in cursor:
    cur_city = row2[0]
    if (cur_city in eng_ru_city_dict.keys()):
        cur_city = eng_ru_city_dict[cur_city]

    if (cur_city in geoRegions and row2[1] in geoRegions):
        if (geoRegions[cur_city]['region'], geoRegions[row2[1]]['region']) not in city_communications:
            city_communications[(geoRegions[cur_city]['region'], geoRegions[row2[1]]['region'])] = 0
            regions_amnt[(geoRegions[cur_city]['region'], geoRegions[row2[1]]['region'])] = 0
            region2_text[(geoRegions[cur_city]['region'], geoRegions[row2[1]]['region'])] = []
        messages = []
        documentA = ' '.join(filter(lambda x: not exclude(x), tt.tokenize(row2[4])))
        documentA = tt.tokenize(documentA)
        documentA = ' '.join([(morph.parse(w)[0]).normal_form for w in documentA])
        counter += 1
        if (counter % 10000 == 0):
            print(counter)
        messages.append(documentA)
        region2_text[(geoRegions[cur_city]['region'], geoRegions[row2[1]]['region'])].append(documentA)
print("CORPUS GENERATED!")
####





used_regs = ["Москва","Санкт-Петербург"]
for key, value in region2_text.items():
    region_text_list = []
    region_in_text_list = []
    if (key[0] not in used_regs):
        used_regs.append(key[0])
        print(key[0])
        for key2, value2 in region2_text.items():
            if (key2[0] == key[0] and key2[1] != key[0]):
                region_text_list += value2
        for key2, value2 in region2_text.items():
            if (key2[0] == key[0] and key2[1] == key[0]):
                region_in_text_list += value2
        print()
        countAll(region_in_text_list, region_text_list, key)


time.sleep(100000)