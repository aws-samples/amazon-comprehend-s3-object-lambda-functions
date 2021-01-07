SHELL := /bin/sh
PY_VERSION := 3.8

export PYTHONUNBUFFERED := 1

SRC_DIR := src
SAM_DIR := .aws-sam

# Required environment variables (user must override)

# S3 bucket used for packaging SAM templates
PACKAGE_BUCKET ?= banner-comprehend-lambdas

# user can optionally override the following by setting environment variables with the same names before running make

# Path to system pip
PIP ?= pip
# Default AWS CLI region
AWS_DEFAULT_REGION ?= us-west-2

PYTHON := $(shell /usr/bin/which python$(PY_VERSION))

.DEFAULT_GOAL := build

clean:
	rm -f $(SRC_DIR)/requirements.txt
	rm -rf $(SAM_DIR)

# used once just after project creation to lock and install dependencies
bootstrap:
	$(PYTHON) -m $(PIP) install pipenv --user
	pipenv lock
	pipenv sync --dev

# used by CI build to install dependencies
init:
	$(PYTHON) -m $(PIP) install aws-sam-cli --user
	$(PYTHON) -m $(PIP) install pipenv --user
	pipenv sync --dev

integ-testing:
	pipenv run py.test -vv test/integ/integ.py

testing:
	pipenv run flake8 $(SRC_DIR)
	pipenv run pydocstyle $(SRC_DIR)
	pipenv run cfn-lint access-control-template.yml
	pipenv run cfn-lint redaction-template.yml
	pipenv run py.test --cov=$(SRC_DIR) --cov-fail-under=95 -vv test/unit -s --cov-report html
	pipenv lock --requirements > $(SRC_DIR)/requirements.txt

compile-redaction: testing
	sam build --template redaction-template.yml
	mv $(SAM_DIR)/build/template.yaml $(SAM_DIR)/build/redaction-template.yml

compile-access-control: testing
	sam build --template access-control-template.yml
	mv $(SAM_DIR)/build/template.yaml $(SAM_DIR)/build/access-control-template.yml

build: compile-redaction

package-redaction: compile-redaction
	sam package --template $(SAM_DIR)/build/redaction-template.yml --s3-bucket $(PACKAGE_BUCKET) --output-template-file $(SAM_DIR)/packaged-redaction-template.yml

deploy-redaction: package-redaction
	sam deploy --region us-east-1 --template-file $(SAM_DIR)/packaged-redaction-template.yml --capabilities CAPABILITY_IAM --stack-name redaction-lambda

publish-redaction: package-redaction
	sam publish  --region us-east-1 --template $(SAM_DIR)/packaged-redaction-template.yml

package-access-control: compile-access-control
	sam package --template $(SAM_DIR)/build/access-control-template.yml --s3-bucket $(PACKAGE_BUCKET) --output-template-file $(SAM_DIR)/packaged-access-control-template.yml

deploy-access-control: package-access-control
	sam deploy --region us-east-1 --template-file $(SAM_DIR)/packaged-access-control-template.yml --capabilities CAPABILITY_IAM --stack-name access-control-lambda

publish-access-control: package-access-control
	sam publish --region us-east-1 --template $(SAM_DIR)/packaged-access-control-template.yml
