SHELL := /bin/sh
PY_VERSION := 3.8

export PYTHONUNBUFFERED := 1

SRC_DIR := src
SAM_DIR := .aws-sam

# Required environment variables (user must override)

# S3 bucket used for packaging SAM templates
PACKAGE_BUCKET ?= comprehend-s3-object-lambdas

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
	$(PYTHON) -m $(PIP) install pipenv
	pipenv lock
	pipenv sync --dev

# used by CI build to install dependencies
init:
	$(PYTHON) -m $(PIP) install aws-sam-cli
	$(PYTHON) -m $(PIP) install pipenv
	pipenv sync --dev
	pipenv lock --requirements > $(SRC_DIR)/requirements.txt

build:
	pipenv run flake8 $(SRC_DIR)
	pipenv run pydocstyle $(SRC_DIR)
	pipenv run cfn-lint $(LAMBDA_NAME)-template.yml
	sam build --profile sar-account --template $(LAMBDA_NAME)-template.yml
	mv $(SAM_DIR)/build/template.yaml $(SAM_DIR)/build/$(LAMBDA_NAME)-template.yml

unit-testing: build
	pipenv run py.test --cov=$(SRC_DIR) --cov-fail-under=97 -vv test/unit -s --cov-report html

# can be triggered as `make integ-testing LAMBDA_NAME=access-control`
integ-testing: unit-testing
	pipenv run py.test  -s -vv test/integ/test_$(LAMBDA_NAME).py

load-testing:
	pipenv run py.test  -s -vv test/load/$(LAMBDA_NAME)_load_test.py --log-cli-level=INFO

package:
	sam package --region us-east-1 --profile sar-account --template $(SAM_DIR)/build/$(LAMBDA_NAME)-template.yml --s3-bucket $(PACKAGE_BUCKET) --output-template-file $(SAM_DIR)/packaged-$(LAMBDA_NAME)-template.yml

deploy: package
	sam deploy --profile sar-account --region us-east-1 --template-file $(SAM_DIR)/packaged-$(LAMBDA_NAME)-template.yml --capabilities CAPABILITY_IAM --stack-name $(LAMBDA_NAME)-lambda

publish: package
	sam publish  --region us-east-1 --template $(SAM_DIR)/packaged-$(LAMBDA_NAME)-template.yml --profile sar-account
