# ==============================================================================
# PJM Load Forecasting - Makefile
# ==============================================================================

.PHONY: all clean rawdata predictions help

# Default target: run full analysis pipeline
all: models/hourly_load_models.rds models/peak_hour_models.rds models/peak_day_models.rds

# ==============================================================================
# Help
# ==============================================================================

help:
	@echo "PJM Load Forecasting - Available targets:"
	@echo ""
	@echo "  make              - Run full analysis (download data, train models)"
	@echo "  make clean        - Delete processed data and models (keep raw data)"
	@echo "  make rawdata      - Delete and re-download raw training data"
	@echo "  make predictions  - Make predictions for tomorrow"
	@echo "  make help         - Show this help message"
	@echo ""

# ==============================================================================
# Data Download
# ==============================================================================

# Download raw training data (PJM load archive + historical weather)
data/raw/.downloaded: analyses/load_data.R analyses/download_historical_weather.R data/external/pjm_airport_anchors.csv
	@echo "===================================================================="
	@echo "Downloading raw training data..."
	@echo "===================================================================="
	Rscript analyses/load_data.R
	@touch data/raw/.downloaded

# ==============================================================================
# Feature Engineering
# ==============================================================================

# Create features from raw data
data/processed/modeling_features.rds: data/raw/.downloaded analyses/create_features.R
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

# Clean: Remove processed data and models (keep raw data)
clean:
	@echo "===================================================================="
	@echo "Cleaning processed data and models..."
	@echo "===================================================================="
	rm -rf data/processed
	rm -rf models
	rm -f data/raw/.downloaded

# Clean raw data: Delete and prepare to re-download
rawdata: clean
	@echo "===================================================================="
	@echo "Deleting raw data (will re-download on next make)..."
	@echo "===================================================================="
	rm -rf data/raw

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