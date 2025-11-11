suppressMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(zoo)  # For rollmean/rollmax
})

# ==============================================================================
# Feature Engineering for PJM Load Forecasting
# ==============================================================================

create_modeling_features <- function(
    load_file = NULL,
    weather_file = "data/raw/weather/historical/openmeteo_november_zone_2019_2022_2023_2024.csv",
    output_file = "data/processed/modeling_features.rds",
    filter_timezone = "America/New_York") {
  
  # Auto-detect PJM load file if not specified
  if (is.null(load_file)) {
    cat("Searching for PJM load file...\n")
    csv_files <- list.files("data/raw", pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
    
    # Exclude weather files
    csv_files <- csv_files[!grepl("weather", csv_files)]
    
    if (length(csv_files) == 0) {
      stop("No CSV files found in data/raw/. Have you run 'make rawdata'?")
    }
    
    # Try to find file with 'zone' and 'load' columns
    for (f in csv_files) {
      tryCatch({
        peek <- read_csv(f, n_max = 1, show_col_types = FALSE)
        if ("zone" %in% names(peek) && "load" %in% names(peek)) {
          load_file <- f
          cat("Found PJM load file:", f, "\n")
          break
        }
      }, error = function(e) {})
    }
    
    if (is.null(load_file)) {
      stop("Could not find PJM load file. Looked for file with 'zone' and 'load' columns.")
    }
  }
  
  cat("Loading data...\n")
  cat("  Load file:", load_file, "\n")
  cat("  Weather file:", weather_file, "\n\n")
  
  # Check files exist
  if (!file.exists(load_file)) {
    stop("Load file not found: ", load_file)
  }
  if (!file.exists(weather_file)) {
    stop("Weather file not found: ", weather_file)
  }
  
  # Read PJM load data
  load_data <- read_csv(load_file, show_col_types = FALSE)
  cat("  Loaded", nrow(load_data), "rows of load data\n")
  
  # Read historical weather data
  weather_data <- read_csv(weather_file, show_col_types = FALSE)
  cat("  Loaded", nrow(weather_data), "rows of weather data\n\n")
  
  # Parse dates and filter to November of target years
  cat("Filtering to November 2019, 2022, 2023, 2024...\n")
  load_data <- load_data |>
    mutate(
      datetime = ymd_hms(datetime, tz = filter_timezone),
      year = year(datetime),
      month = month(datetime)
    ) |>
    filter(
      year %in% c(2019, 2022, 2023, 2024),
      month == 11
    )
  cat("  Filtered to", nrow(load_data), "rows\n\n")
  
  # Parse weather dates
  weather_data <- weather_data |>
    mutate(
      time_ept = ymd_hms(time_ept, tz = filter_timezone)
    )
  
  # Join load and weather
  cat("Joining load and weather data...\n")
  modeling_data <- load_data |>
    left_join(
      weather_data,
      by = c("zone" = "load_area", "datetime" = "time_ept")
    )
  cat("  Joined data has", nrow(modeling_data), "rows\n\n")
  
  # Create temporal features
  cat("Creating temporal features...\n")
  modeling_data <- modeling_data |>
    mutate(
      # Time features
      hour = hour(datetime),
      day_of_week = wday(datetime, week_start = 1),  # 1=Monday
      day_of_month = day(datetime),
      is_weekend = day_of_week %in% c(6, 7),
      
      # Holiday flag (approximate - Thanksgiving week)
      is_holiday = (day_of_month >= 24 & day_of_month <= 28),
      
      # Hour categories
      hour_category = case_when(
        hour >= 0 & hour < 6 ~ "night",
        hour >= 6 & hour < 12 ~ "morning",
        hour >= 12 & hour < 18 ~ "afternoon",
        hour >= 18 & hour < 24 ~ "evening"
      )
    )
  
  # Create lag features (by zone)
  cat("Creating lag features...\n")
  modeling_data <- modeling_data |>
    arrange(zone, datetime) |>
    group_by(zone) |>
    mutate(
      # Lag features
      load_lag1 = lag(load, 1),      # Previous hour
      load_lag24 = lag(load, 24),    # Same hour yesterday
      load_lag168 = lag(load, 168),  # Same hour last week
      
      # Rolling averages
      load_roll_mean_24 = rollmean(load, k = 24, fill = NA, align = "right"),
      load_roll_max_24 = rollmax(load, k = 24, fill = NA, align = "right"),
      
      # Temperature lags
      temp_lag1 = lag(temp_mean, 1),
      temp_lag24 = lag(temp_mean, 24)
    ) |>
    ungroup()
  
  # Create interaction features
  cat("Creating interaction features...\n")
  modeling_data <- modeling_data |>
    mutate(
      # Temperature-time interactions
      temp_hour = temp_mean * hour,
      CDH_hour = CDH_mean * hour,
      HDH_hour = HDH_mean * hour,
      
      # Weekend-temperature interaction
      temp_weekend = temp_mean * as.numeric(is_weekend)
    )
  
  # Add date identifier (for splitting train/test)
  modeling_data <- modeling_data |>
    mutate(
      date = as.Date(datetime),
      year_month = sprintf("%d-%02d", year, month)
    )
  
  # Create zone-specific statistics (for normalization)
  cat("Computing zone statistics...\n")
  zone_stats <- modeling_data |>
    group_by(zone) |>
    summarize(
      load_mean = mean(load, na.rm = TRUE),
      load_sd = sd(load, na.rm = TRUE),
      load_min = min(load, na.rm = TRUE),
      load_max = max(load, na.rm = TRUE)
    )
  
  # Add zone statistics to data
  modeling_data <- modeling_data |>
    left_join(zone_stats, by = "zone")
  
  # Save processed data
  cat("Saving processed data...\n")
  dir.create(dirname(output_file), showWarnings = FALSE, recursive = TRUE)
  saveRDS(list(
    data = modeling_data,
    zone_stats = zone_stats
  ), output_file)
  
  # Print summary
  cat("\n=== Feature Engineering Complete ===\n")
  cat("Rows:", nrow(modeling_data), "\n")
  cat("Zones:", n_distinct(modeling_data$zone), "\n")
  cat("Date range:", format(min(modeling_data$datetime)), "to", format(max(modeling_data$datetime)), "\n")
  cat("Missing values in key columns:\n")
  key_cols <- c("load", "temp_mean", "load_lag24", "load_lag168")
  for (col in key_cols) {
    if (col %in% names(modeling_data)) {
      cat("  ", col, ":", sum(is.na(modeling_data[[col]])), "\n")
    }
  }
  cat("\nSaved to:", output_file, "\n")
  
  invisible(list(
    data = modeling_data,
    zone_stats = zone_stats
  ))
}

# ==============================================================================
# If run directly
# ==============================================================================

if (!interactive()) {
  create_modeling_features()
}