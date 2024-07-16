all: check_convention unittest
.PHONY: all test build test_py2 test_py3 lint_py2 lint_py3
ARTIFACT=dist/racattack_api-*-py2.py3-none-any.whl
all: lint test build

test: test_py2 test_py3

test_py3:
	PYTHONPATH=py python3 -m coverage run py/rackattack/tests/runner.py
	python3 -m coverage report --show-missing --include=py/*.py --omit=py/rackattack/tests/*.py,*/__init__.py

test_py2:
	PYTHONPATH=py python2.7 -m coverage run py/rackattack/tests/runner.py
	python2.7 -m coverage report --show-missing --include=py/*.py --omit=py/rackattack/tests/*.py,*/__init__.py

lint: lint_py2 lint_py3

lint_py3:
	python3 -m pep8 py --max-line-length=120

lint_py2:
	python2.7 -m pep8 py --max-line-length=120

build: $(ARTIFACT)

clean:
	find . -name *.pyc -delete
	find . -name __pycache__ -delete
	rm -rf dist */*.egg-info build *.stratolog

$(ARTIFACT):
	python3 -m build --wheel

#this can not run in dirbalak clean build, as solvent can not yet be run at this point (still running in 
#rootfs-build-nostrato, this project is a dependency of rootfs-buid). This is why this is actually being
#run in pyracktest
delayed_racktest:
	PYTHONPATH=$(PWD):$(PWD)/py python test/test.py $(TESTS)
virttest:
	RACKATTACK_PROVIDER=tcp://localhost:1014@@amqp://guest:guest@localhost:1013/%2F@@http://localhost:1016 $(MAKE) delayed_racktest
phystest:
	RACKATTACK_PROVIDER=tcp://rackattack-provider.dc1.strato:1014@@amqp://guest:guest@rackattack-provider.dc1.strato:1013/%2F@@http://rackattack-provider.dc1.strato:1016 $(MAKE) delayed_racktest
