docker-build:
	docker build -t pyspark-regression:1.0 .

test: docker-build
	docker run -it --rm --entrypoint pytest pyspark-regression:1.0

build: test
	rm -rf dist
	pip install build
	python -m build

deploy-dev: build
	pip install twine
	python -m twine upload --repository testpypi dist/*

deploy: test
	python -m twine upload dist/*