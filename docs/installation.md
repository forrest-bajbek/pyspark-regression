# Installation

???+ warning
    `pyspark-regression` requires a working installation of Spark 3+ and `pyspark`

    - You can install Spark with brew:
    ```bash
    brew install apache-spark@3.3.1
    ```
        - Note: Apache Spark depends on `openjdk`. You may have to do some extra configuration. Running `brew install openjdk` should print any additional instructions.
    - You can install `pyspark` with pip:
    ```bash
    pip install pyspark==3.3.1
    ```
        - Make sure your version of `pyspark` matches your version of Spark!

## Via pip
You can install via pip:
```bash
pip install pyspark-regression
```

## Via Git
To install via git:
```bash
git clone https://github.com/forrest-bajbek/pyspark-regression.git
cd pyspark-regression
pip install .
```

## Via Docker
To build a Docker Image:
```bash
git clone https://github.com/forrest-bajbek/pyspark-regression.git
cd pyspark-regression
docker build -t pyspark-regression:1.1 .
```