init:
	brew install apache-spark@3.3.1
	pip install -e .[dev]

test-cov: init
	python -m pytest --cov-config=.coveragec --cov=src

test: init
	python -m pytest

build: test
	rm -rf dist
	python -m build

deploy-dev: build
	python -m twine upload --repository testpypi dist/*

deploy: test
	python -m twine upload dist/*

docker-build:
	docker build -t pyspark-regression:1.1 .

docker-test: docker-build
	docker run -it --rm --entrypoint /bin/sh pyspark-regression:1.1 -c "python3 -m pytest"