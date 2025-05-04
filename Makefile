init:
	brew install apache-spark \
	&& rm -rf venv
	&& python -m venv venv
	&& source venv/bin/activate
	&& pip install -e ".[dev]"

test:
	python -m pytest

test-cov:
	python -m pytest --cov-config=.coveragec --cov=src

build: test
	rm -rf dist
	python -m build

deploy-dev: build
	python -m twine upload --repository testpypi dist/*

deploy: build
	python -m twine upload dist/*

deploy-docs:
	mkdocs gh-deploy
