.PHONY: help clean clean-build clean-pyc dist install develop

PYTHON?=python
PIP?=pip

help:
	@echo "tabun_feed"
	@echo
	@echo "clean - remove all build and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "dist - package"
	@echo "install - install the package to the active Python's site-packages"
	@echo "develop - install the package for development as editable"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr *.egg-info
	rm -fr *.egg

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

dist: clean
	$(PYTHON) setup.py sdist
	ls -l dist

install: clean
	$(PIP) install .

develop:
	$(PIP) install -r requirements.txt -r optional-requirements.txt
	$(PIP) install -e .
