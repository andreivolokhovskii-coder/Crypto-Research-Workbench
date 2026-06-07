FROM apache/superset:3.0.3

USER root

RUN pip install --no-cache-dir \
    clickhouse-sqlalchemy==0.2.5 \
    clickhouse-connect==0.6.23

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER superset

ENTRYPOINT ["/entrypoint.sh"]
