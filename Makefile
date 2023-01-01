docker-build:
	docker build -t pyspark-regression:3.3 .

test: docker-build
	docker run -it --rm --entrypoint pytest pyspark-regression:3.3

build: test
	pip install build
	python -m build

deploy-dev: build
	pip install twine
	python -m twine upload --repository testpypi dist/*

deploy: test
	python -m twine dist/*