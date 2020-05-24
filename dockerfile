FROM python:3.7
RUN mkdir /app && \
mkdir /dbt_kimball_dimensional && \
mkdir /root/.dbt
COPY test_dbt_kimball_dimensional/profiles.yml /root/.dbt/profiles.yml
COPY test_dbt_kimball_dimensional /app
WORKDIR /app
RUN apt-get update -y && pip3 install -r requirements.txt

