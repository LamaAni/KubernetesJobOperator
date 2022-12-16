FROM apache/airflow

RUN pip3 install --user airflow-kubernetes-job-operator

COPY --chown=airflow webserver_config.py .
COPY --chown=airflow start_airflow .

RUN chmod +x ./start_airflow

# Changed the port so not to collide with local
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

ENTRYPOINT []
CMD [ "./start_airflow" ]