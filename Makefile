
build_dist:
	python setup.py sdist bdist_wheel

upload_test: build_dist
	python -m twine upload --repository testpypi dist/*

upload_pypi: build_dist
	python -m twine upload dist/*

condabuild:
	mkdir condabuild

condabuild/spherpro/meta.yaml: condabuild
	cd condabuild;\
		conda skeleton pypi spherpro

conda_build: condabuild/spherpro/meta.yaml
	cd condabuild && mamba build -c votti -c conda-forge spherpro



