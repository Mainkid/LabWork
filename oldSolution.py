import json
import csv
import nltk
import numpy
import psycopg2
import sys
import csv
import gzip
import string


import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer

from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import pymorphy2




morph = pymorphy2.MorphAnalyzer()

nltk.download('stopwords')

s_ru = set(stopwords.words('russian'))
s_en = set(stopwords.words('english'))
tt = TweetTokenizer()

def countAll(text_in,text_out,corpus_set,region):
    vectorizer2 = TfidfVectorizer()
    vectors2 = None
    vectors2 = vectorizer2.fit(corpus_set)
    if (len(text_out)==0):
        return
    vectors2 = vectorizer2.transform(text_out)
    feature_names = vectorizer2.get_feature_names_out()
    is_used = False
    dop_counter=1
    for i in range(0,vectors2.get_shape()[0],200):
        print("Converting To Dense...")
        dense = vectors2[i:min(i+200,vectors2.get_shape()[0]),:].todense()
        print("Converting Dense to list...")
        denselist = dense.tolist()
        print("Done...")
        df = pd.DataFrame(denselist, columns=feature_names)
        df_mean = df.mean().to_frame()
        if (not is_used):
            values_0 = pd.DataFrame(df_mean[0])
            is_used=True
        else:
            values_0=pd.concat([values_0,df_mean],axis=1)
            #values_0[dop_counter]=df_mean[0]
            dop_counter+=1
            #values_0 = numpy.vstack([values_0,df_mean[0].sort_values(ascending=False)])
            print(values_0.shape)

    values_0=pd.DataFrame(values_0)
    values_0=values_0.mean(axis=1).to_frame()

    print("RES SHAPE: ")
    print(values_0.shape)
    print("1 STAGE")
    #values_0=(values_0.mean().to_frame())
    values_0=values_0[0].sort_values(ascending=False)
    print(values_0)
    dop_counter=1
    #dense = vectors2.todense()

    #denselist = dense.tolist()

    #df = pd.DataFrame(denselist, columns=feature_names)
    #df_mean = df.mean().to_frame()

    #values_0 = df_mean[0].sort_values(ascending=False)


    sorted_0 = list(sorted(values_0.to_dict().items(), key=lambda x: -x[1]))


    top_0 = sorted_0[:1000]

    totalList = [["Word","Value"]]
    for vals in top_0:
        list2 = []
        if (vals[1]==0):
            break
        list2.append(vals[0])
        list2.append(vals[1])
        totalList.append(list2)
    with open(r"C:\Users\jdczy\Desktop\Diploma\Coursework\web_app\Analys\FileStorage\RES_OUT_" + region[0] + ".csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(totalList)


    ###2

    print("2 STAGE")
    is_used = False
    if (len(text_in)==0):
        return
    vectors2 = vectorizer2.transform(text_in)
    feature_names = vectorizer2.get_feature_names_out()




    for i in range(0, vectors2.get_shape()[0], 200):
        dense = vectors2[i:min(i + 200, vectors2.get_shape()[0]), :].todense()
        print(i)
        denselist = dense.tolist()

        df = pd.DataFrame(denselist, columns=feature_names)
        df_mean = df.mean().to_frame()
        if (not is_used):
            values_0 = pd.DataFrame(df_mean[0])
            is_used = True
        else:
            values_0=pd.concat([values_0, df_mean],axis=1)
            #values_0[dop_counter] = df_mean[0]
            dop_counter += 1
            # values_0 = numpy.vstack([values_0,df_mean[0].sort_values(ascending=False)])
            print(values_0.shape)

    values_0 = pd.DataFrame(values_0)
    values_0=values_0.mean(axis=1).to_frame()
    #print("RES SHAPE: "+values_0.shape)
    values_0 = values_0[0].sort_values(ascending=False)



    print("3 STAGE")
    sorted_0 = list(sorted(values_0.to_dict().items(), key=lambda x: -x[1]))


    top_0 = sorted_0[:1000]

    totalList = [["Word","Value"]]
    for vals in top_0:
        list2 = []
        if (vals[1]==0):
            break
        list2.append(vals[0])
        list2.append(vals[1])
        totalList.append(list2)
    with open(r"C:\Users\jdczy\Desktop\Diploma\Coursework\web_app\Analys\FileStorage\RES_IN_" + region[0] + ".csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(totalList)



    print("DONE REGION!")

def connectToDB():
    global cursor
    global connection
    with open(r'C:\Users\jdczy\Desktop\Diploma\Coursework\web_app\Analys\city_regions.json', encoding='utf8') as f:
        city_BD = json.load(f)

    connection = psycopg2.connect(user="postgres", database="postgres",
                                    # пароль, который указали при установке PostgreSQL
                                    password="Samsung01",
                                    host="localhost",
                                    port="5432")

    print("Connected successfuly!")
    cursor = connection.cursor()
    return connection

def ispunct(ch):
    return ch in string.punctuation

def exclude(s):
    return len(s) < 4 or ispunct(s[0]) or s[0].isdigit() or s in s_ru or s in s_en or s[:2] == "id"

def getTopWords():
    connection=connectToDB()
    with open('cities_ru_en.csv', mode='r',encoding="utf-8") as inp:
        reader = csv.reader(inp)
        eng_ru_city_dict = {rows[1]:rows[0] for rows in reader}

    city_communications={}

    region_communications_pos={}
    region_communications_neg={}
    corpus_set=[]
    region2_text={}



    regions_amnt={}
    regions_scalar_sentiment={}
    get_first_city = "SELECT object_comments.id, object_comments.id_from, title,text " \
                     "FROM object_comments,user_data_table,city " \
                     "WHERE object_comments.id=user_data_table.id AND user_data_table.city_id IS NOT NULL " \
                     "AND user_data_table.city_id=city.id_city LIMIT 2000000"



    cursor = connection.cursor()
    cursor.execute(get_first_city)
    counter=0
    with open(r'C:\Users\jdczy\Desktop\Diploma\Coursework\web_app\Analys\city_regions.json', encoding='utf8') as f:
        geoRegions = json.load(f)
    for row in cursor:
        check_null_cursor = connection.cursor()
        check_null = "SELECT title FROM user_data_table,city WHERE id=" + str(row[1]) + " AND city_id=id_city"
        check_null_cursor.execute(check_null)

        for row2 in check_null_cursor:
            cur_city = row2[0]
            if (cur_city in eng_ru_city_dict.keys()):
                cur_city = eng_ru_city_dict[cur_city]

            if (cur_city in geoRegions and row[2] in geoRegions):
                if (geoRegions[cur_city]['region'], geoRegions[row[2]]['region']) not in city_communications:
                    city_communications[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])] = 0
                    region_communications_pos[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])] = 0
                    region_communications_neg[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])] = 0
                    regions_amnt[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])] = 0
                    regions_scalar_sentiment[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])] = 0
                    region2_text[(geoRegions[cur_city]['region'], geoRegions[row[2]]['region'])]=[]
                messages = []
                documentA = ' '.join(filter(lambda x: not exclude(x), tt.tokenize(row[3])))
                documentA = tt.tokenize(documentA)
                documentA = ' '.join([(morph.parse(w)[0]).normal_form for w in documentA])
                counter+=1
                if (counter % 10000==0):
                    print(counter)
                corpus_set.append(documentA)
                messages.append(documentA)
                region2_text[(geoRegions[cur_city]['region'],geoRegions[row[2]]['region'])].append(documentA)
    print("CORPUS GENERATED!")
    ####
    ####
    vectorizer = TfidfVectorizer()
    vectors = vectorizer.fit(corpus_set)
    vectors = vectorizer.transform(corpus_set)



    #feature_names = vectorizer.get_feature_names_out()
    #is_used=False
    #for i in range(0,200,100):
    #    dense = vectors[i:i+100,:].todense()

    #    denselist = dense.tolist()

    #    df = pd.DataFrame(denselist, columns=feature_names)
    #    df_mean = df.mean().to_frame()
    #    if (not is_used):
    #       values_0 = df_mean[0].sort_values(ascending=False)
    #        is_used=True
    #    else:
    #        values_0 = numpy.vstack([values_0,df_mean[0].sort_values(ascending=False)])

    #print(values_0)
    #values_0=pd.DataFrame(values_0)
    #values_0=(values_0.mean().to_frame())
    #print("OK")
    #values_0=values_0[0].sort_values(ascending=False)
    #print(values_0)
    #sorted_0 = list(sorted(values_0.to_dict().items(), key=lambda x: -x[1]))



    ### Для каждого региона получить внутреннюю и внешнюю тональность
    used_regs=["Москва"]
    for key,value in region2_text.items():
        region_text_list = []
        region_in_text_list = []
        if (key[0] in used_regs):
            used_regs.append(key[0])
            print(key[0])
            for key2,value2 in region2_text.items():
                if (key2[0]==key[0] and key2[1]!=key[0]):
                    region_text_list+=value2
            for key2, value2 in region2_text.items():
                if (key2[0] == key[0] and key2[1] == key[0]):
                    region_in_text_list += value2

            countAll(region_in_text_list,region_text_list,corpus_set,key)


getTopWords()
print()