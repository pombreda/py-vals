
SHELL=/bin/bash

all:

-include local.mk

clean:
	rm -fr *.egg-info dist build

register:
	python setup.py sdist register

upload:
	python setup.py sdist upload
