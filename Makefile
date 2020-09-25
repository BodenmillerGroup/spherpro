clean:
	find . -type d -name __pycache__ -exec rm -r {} \+
	find . -type d -name .pytest_cache -exec rm -r {} \+
	find . -type d -name .mypy_cache -exec rm -r {} \+
	find . -name '*.egg-info' -exec rm -fr {} +
	rm -fr build/
	rm -fr dist/
	rm -fr tests/testdata


build_dist:
	python setup.py sdist bdist_wheel

upload_test:
	python -m twine upload --repository testpypi dist/*

upload:
	python -m twine upload dist/*

check:
	python -m twine check dist/*

black:
	black spherpro