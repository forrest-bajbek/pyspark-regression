# Installation
## Via pip
You can install via pip:
```bash
pip install pyspark-regression==1.0
```
**Note:** This requires a working installation of Spark 3+ and `pyspark>=3`.

## Via Git
To install via git:
```bash
git clone https://github.com/forrest-bajbek/pyspark-regression.git
cd pyspark-regression
pip install .
```
**Note:** This requires a working installation of Spark 3+ and `pyspark>=3`.

## Via Docker
To build and then test the Docker Image:
```bash
git clone https://github.com/forrest-bajbek/pyspark-regression.git
cd pyspark-regression
make test
```