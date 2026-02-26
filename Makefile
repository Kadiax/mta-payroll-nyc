# --- Configuration ---
IMAGE_NAME=mta-pipeline

# OS Detection for Google Cloud Config paths
ifeq ($(OS),Windows_NT)
    # Windows: Credentials typically located in AppData
    GCLOUD_CONFIG_LOCAL = $(APPDATA)/gcloud
else
    # Linux / Mac: Credentials typically located in .config
    GCLOUD_CONFIG_LOCAL = ~/.config/gcloud
endif

# Internal container path for Application Default Credentials (ADC)
ADC_INTERNAL = /root/.config/gcloud/application_default_credentials.json

.PHONY: build create-datasets extract load test all

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run unit tests inside the container
test:
	docker run --rm $(IMAGE_NAME) pytest tests/

# Run dataset creation
create-datasets:
	docker run --rm -v "$(GCLOUD_CONFIG_LOCAL):/root/.config/gcloud" -e GOOGLE_APPLICATION_CREDENTIALS=$(ADC_INTERNAL) $(IMAGE_NAME) python -m scripts.create_datasets

# Run extraction and anonymization
extract:
	docker run --rm -v "$(GCLOUD_CONFIG_LOCAL):/root/.config/gcloud" -e GOOGLE_APPLICATION_CREDENTIALS=$(ADC_INTERNAL) $(IMAGE_NAME) python -m scripts.extract_and_anonymize

# Run BigQuery loading
load:
	docker run --rm -v "$(GCLOUD_CONFIG_LOCAL):/root/.config/gcloud" -e GOOGLE_APPLICATION_CREDENTIALS=$(ADC_INTERNAL) $(IMAGE_NAME) python -m scripts.load_to_bq

transform:
	docker run --rm -v "$(GCLOUD_CONFIG_LOCAL):/root/.config/gcloud" -e GOOGLE_APPLICATION_CREDENTIALS=$(ADC_INTERNAL) $(IMAGE_NAME) python -m scripts.transform

# Execute the full pipeline: Build -> Test -> Run
all: build test create-datasets extract load transform