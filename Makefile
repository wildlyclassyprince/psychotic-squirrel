.PHONY: help test-data-file pipeline install

# Date range parameters
START_DATE ?=
END_DATE ?=

test: ## Run test command
	@echo "this is a test"

help: ## Display this help message
	@echo 'Usage:'
	@echo 'make install'
	@echo 'make db-ready'
	@echo 'make sample-data-file'
	@echo 'make run-pipeline [START_DATE=YYYY-MM-DD] [END_DATE=YYYY-MM-DD]'
	@echo ''
	@echo 'Targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort  | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install challenge dependencies in virtual environment
	pip install -U pip -r requirements.txt

db-ready: ## Extract database from zip file
	unzip data/db/challenge.zip -d data/db/

sample-data-file: ## Generate training data for training attribution model
	python pipeline/generate_training_data.py

run-pipeline: ## Run attribution pipeline with optional date range
	python pipeline/attribution_orchestration.py $(if $(START_DATE),--start-date $(START_DATE)) $(if $(END_DATE),--end-date $(END_DATE))

.DEFAULT_GOAL := help
