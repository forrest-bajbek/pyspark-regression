FROM apache/spark-py:3.3.1

# set working directory
WORKDIR /usr/src/pyspark-regression

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install python dependencies
USER root
COPY ./requirements.txt .
COPY ./requirements-dev.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements-dev.txt

# add app
USER ${spark_uid}
COPY . .
RUN pip install .

# add entrypoint
ENTRYPOINT ["/opt/spark/bin/pyspark"]