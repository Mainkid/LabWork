FROM python:3.6


RUN pip install psycopg2
RUN pip install nltk
RUN pip install pymorphy2
RUN pip install scikit-learn
RUN pip install pandas
COPY . /usr/src/labwork/

WORKDIR /usr/src/labwork/

ENV PYTHONPATH /usr/src

EXPOSE 5432

CMD ["python","-u","GetFrequency.py"]