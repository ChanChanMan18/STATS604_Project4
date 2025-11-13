# ==============================================================================
# PJM Load Forecasting - Makefile
# ==============================================================================

# ==============================================================================
# Environment Variables
# ==============================================================================

# PJM API Key (required for downloading current load data)
# Set this to your actual API key or set PJM_API_KEY environment variable
PJM_API_KEY ?= c7945781dabb40f39f56bbe4601d677e

# Timezone for all operations
PJM_TZ ?= America/New_York

# User agent for API requests
NWS_USER_AGENT ?= power-forecast/1.0

# Export variables so they're available to all commands
export PJM_API_KEY
export PJM_TZ
export NWS_USER_AGENT

# ==============================================================================
# Phony Targets
# ==============================================================================

.PHONY: all clean rawdata predictions help

# Default target: run full analysis pipeline (assumes raw data already exists)
all: models/hourly_load_models.rds models/peak_hour_models.rds models/peak_day_models.rds

# ==============================================================================
# Help
# ==============================================================================

help:
	@echo "PJM Load Forecasting - Available targets:"
	@echo ""
	@echo "  make              - Run all analyses (feature engineering + training)"
	@echo "                      NOTE: Assumes raw data already exists in Docker image"
	@echo "  make clean        - Delete processed data and models (keep raw data/code)"
	@echo "  make rawdata      - Delete and re-download raw training data"
	@echo "  make predictions  - Make predictions for tomorrow"
	@echo "  make help         - Show this help message"
	@echo ""
	@echo "Environment Variables:"
	@echo "  PJM_API_KEY       - PJM DataMiner API key (required for predictions)"
	@echo "  PJM_TZ            - Timezone (default: America/New_York)"
	@echo "  NWS_USER_AGENT    - User agent for API requests"
	@echo ""
	@echo "Example usage with API key:"
	@echo "  PJM_API_KEY=your_key_here make predictions"
	@echo ""

# ==============================================================================
# Data Download (for make rawdata only)
# ==============================================================================

# Download raw training data (PJM load archive + historical weather)
# This is only called by 'make rawdata', not by default 'make'
.PHONY: download_raw_data
download_raw_data: analyses/load_data.R analyses/download_historical_weather.R data/external/pjm_airport_anchors.csv
	@echo "===================================================================="
	@echo "Downloading raw training data..."
	@echo "===================================================================="
	Rscript analyses/load_data.R

# ==============================================================================
# Feature Engineering
# ==============================================================================

# Create features from raw data (assumes data already exists)
data/processed/modeling_features.rds: analyses/create_features.R
	@echo "===================================================================="
	@echo "Creating features..."
	@echo "===================================================================="
	Rscript analyses/create_features.R

# ==============================================================================
# Model Training
# ==============================================================================

# Train hourly load forecasting models
models/hourly_load_models.rds: data/processed/modeling_features.rds analyses/train_models.R
	@echo "===================================================================="
	@echo "Training hourly load models..."
	@echo "===================================================================="
	Rscript analyses/train_models.R

# Train peak hour and peak day models
models/peak_hour_models.rds models/peak_day_models.rds: models/hourly_load_models.rds analyses/train_peak_models.R
	@echo "===================================================================="
	@echo "Training peak hour and peak day models..."
	@echo "===================================================================="
	Rscript analyses/train_peak_models.R

# ==============================================================================
# Predictions
# ==============================================================================

# Make predictions for tomorrow
predictions: models/hourly_load_models.rds models/peak_hour_models.rds models/peak_day_models.rds analyses/make_predictions.R analyses/download_pjm_load_current.R analyses/download_historical_weather_current.R analyses/download_weather_forecast.R
	@Rscript analyses/make_predictions.R 2>/dev/null

# ==============================================================================
# Cleanup
# ==============================================================================

# Clean: Remove processed data and models (keep raw data and code)
clean:
	@echo "===================================================================="
	@echo "Cleaning processed data and models..."
	@echo "===================================================================="
	rm -rf data/processed
	rm -rf models

# Clean raw data and re-download
rawdata: clean
	@echo "===================================================================="
	@echo "Deleting and re-downloading raw training data..."
	@echo "===================================================================="
	rm -rf data/raw
	$(MAKE) download_raw_data

# ==============================================================================
# Notes
# ==============================================================================

# Directory structure:
#   data/
#     external/          - Anchor points CSV (not deleted)
#     raw/              - Downloaded PJM load and weather data
#     processed/        - Engineered features
#   models/             - Trained models
#   analyses/           - R scripts
#   predictions.csv     - Daily predictions (committed to git)