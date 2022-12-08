import re
import json
import numpy as np
import nltk
import psycopg2
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import gensim.downloader as api
from gensim.models import TfidfModel
from gensim import corpora
from gensim.utils import simple_preprocess
from gensim.corpora import Dictionary
import pymorphy2
from sklearn.feature_extraction.text import TfidfVectorizer


morph = pymorphy2.MorphAnalyzer()

nltk.download('stopwords')

s_ru = set(stopwords.words('russian'))
s_en = set(stopwords.words('english'))
tt = TweetTokenizer()

get_all_comments= "SELECT text FROM object_comments,user_data_table WHERE city_id is NOT NULL AND user_data_table.id=object_comments.id LIMIT 1000"

connection = psycopg2.connect(user="postgres", database="Coursework",
                                  # пароль, который указали при установке PostgreSQL
                                  password="Samsung01",
                                  host="localhost",
                                  port="5432")

##--------

cursor= connection.cursor()
print("Connected")
cursor.execute(get_all_comments)

#                documentA = ' '.join([(morph.parse(w)[0]).normal_form for w in documentA])
corpus=[' '.join([(morph.parse(w)[0]).normal_form for w in tt.tokenize(re.sub("\[.*\]",'',item[0]))]) for item in cursor.fetchall()]
#corpus=[" ".join(corpus)]
vectorizer = TfidfVectorizer(min_df=0.001)
X= vectorizer.fit_transform(corpus).toarray()
X=X.mean(0)
a=vectorizer.get_feature_names_out()
#corpus = [simple_preprocess(doc) for doc in corpus]
#dictionary = corpora.Dictionary(corpus)
#corpus = [dictionary.doc2bow(doc, allow_update=True) for doc in corpus]
#model = TfidfModel(corpus,smartirs ='ntc')
#weight_tfidf =[]

corpus=dict(zip(a,X))
corpus=dict(sorted(corpus.items(), key=lambda item: item[1],reverse=True))

with open("corpus.json", "w",encoding='utf-8') as fp:
    json.dump(corpus , fp,ensure_ascii=False)