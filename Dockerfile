# ==============================================================================
# PJM Load Forecasting - Dockerfile
# Base image: jupyter/r-notebook (Ubuntu 24, R with Jupyter)
# Platform: linux/amd64
# ==============================================================================

FROM --platform=linux/amd64 jupyter/r-notebook:latest

USER root

# ==============================================================================
# Install system dependencies
# ==============================================================================

RUN apt-get update && apt-get install -y \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ==============================================================================
# Set working directory
# ==============================================================================

WORKDIR /home/jovyan/work

# ==============================================================================
# Install R packages
# ==============================================================================

RUN R -e "install.packages(c( \
    'readr', \
    'dplyr', \
    'tidyr', \
    'lubridate', \
    'httr2', \
    'jsonlite', \
    'purrr', \
    'fs', \
    'zoo', \
    'randomForest' \
), repos='https://cran.rstudio.com/')"

# ==============================================================================
# Copy project files
# ==============================================================================

# Copy external data (anchors file)
COPY data/external/ ./data/external/

# Copy analysis scripts
COPY analyses/ ./analyses/

# Copy Makefile
COPY Makefile .

# ==============================================================================
# Download and prepare training data
# ==============================================================================

# Set environment variables for data download
ENV DATA_DIR=/home/jovyan/work/data/raw
ENV EXT_DIR=/home/jovyan/work/data/external
ENV PJM_TZ=America/New_York
ENV NWS_USER_AGENT=power-forecast/1.0

# Download raw training data (PJM load archive + historical weather)
RUN Rscript analyses/load_data.R

# ==============================================================================
# Run training pipeline (feature engineering + model training)
# ==============================================================================

# Create features from raw data
RUN Rscript analyses/create_features.R

# Train models
RUN Rscript analyses/train_models.R
RUN Rscript analyses/train_peak_models.R

# ==============================================================================
# Set up for predictions
# ==============================================================================

# Change ownership to notebook user
RUN chown -R ${NB_UID}:${NB_GID} /home/jovyan/work

# Switch back to notebook user
USER ${NB_USER}

# Set working directory
WORKDIR /home/jovyan/work

# Default command: bash terminal
CMD ["/bin/bash"]