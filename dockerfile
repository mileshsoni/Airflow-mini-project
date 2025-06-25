from apache/airflow:2.6.3

#copy requirements
COPY requirements.txt /

RUN bash -c "pip install --user --no-cache-dir -r /requirements.txt"

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"

