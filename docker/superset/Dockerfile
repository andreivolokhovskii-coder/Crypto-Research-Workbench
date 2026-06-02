FROM apache/superset:3.0.3

USER root

# Install ClickHouse connector and other useful DB drivers
RUN pip install --no-cache-dir \
    clickhouse-connect>=0.6.0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER superset

ENTRYPOINT ["/entrypoint.sh"]
