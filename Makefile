
build_dist:
	python setup.py sdist bdist_wheel

upload_test:
	python -m twine upload --repository testpypi dist/*

upload:
	python -m twine upload dist/*
