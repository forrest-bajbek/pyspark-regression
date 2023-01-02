FROM apache/spark-py:3.3.1

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# set working directory
WORKDIR /usr/src/pyspark-regression

# install app
USER root
RUN pip install pyspark-regression
USER ${spark_uid}

# add entrypoint
ENTRYPOINT ["/opt/spark/bin/pyspark"]