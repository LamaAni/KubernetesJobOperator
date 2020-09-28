FROM lamaani/zairflow:latest

COPY . /tmp/kjo-installer/
RUN cp /app/airflow.cfg /airflow/airflow.cfg
RUN pip3 install --user /tmp/kjo-installer