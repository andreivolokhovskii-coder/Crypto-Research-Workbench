FROM apache/superset:3.0.3

USER root

# clickhouse-connect 0.7+ requires SQLAlchemy 2.0, Superset 3.x uses 1.4 — pin to last 0.6.x
RUN pip install --no-cache-dir \
    clickhouse-connect==0.6.23

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER superset

ENTRYPOINT ["/entrypoint.sh"]
