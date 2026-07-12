FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER airflow

COPY --chown=airflow:root . /opt/tollkeeper
WORKDIR /opt/tollkeeper

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -e ".[sql]" -e "./packages/airflow-tollkeeper" pytest pytest-cov \
    apache-airflow-providers-apache-spark

ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////home/airflow/airflow.db
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/tollkeeper/test_dags

RUN airflow db migrate

CMD ["pytest", "tests/", "packages/airflow-tollkeeper/tests/", "-v", "--tb=short"]
