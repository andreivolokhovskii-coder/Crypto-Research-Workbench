FROM ghcr.io/dbt-labs/dbt-clickhouse:1.7.1

WORKDIR /usr/app/dbt

# dbt project is mounted at /usr/app/dbt via docker-compose volume.
# profiles.yml uses env_var() to read connection details at runtime.
