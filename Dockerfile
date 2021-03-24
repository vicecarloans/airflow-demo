FROM apache/airflow:1.10.14

RUN pip install --no-cache-dir --user pandas
RUN pip install --no-cache-dir --user gcsfs
RUN pip install --no-cache-dir --user fsspec