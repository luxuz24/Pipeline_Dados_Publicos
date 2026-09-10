FROM apache/airflow:2.10.5-python3.10

COPY extractor/requirements.txt /tmp/extractor-requirements.txt
RUN pip install --no-cache-dir -r /tmp/extractor-requirements.txt