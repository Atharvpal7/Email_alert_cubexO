FROM apache/airflow:2.9.2-python3.10

WORKDIR /opt/airflow

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

