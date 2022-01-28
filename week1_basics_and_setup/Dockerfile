FROM python:3.9 
RUN apt-get install wget
RUN pip install sqlalchemy psycopg2
RUN pip install pandas 

WORKDIR /app
COPY jupytertest.py jupytertest.py
ENTRYPOINT ["python","jupytertest.py"] 
