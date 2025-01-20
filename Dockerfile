FROM apache/airflow:2.10.4
COPY requirements.txt /
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --no-cache-dir -r /requirements.txt

RUN umask 0002; mkdir -p opt/airflow/data
