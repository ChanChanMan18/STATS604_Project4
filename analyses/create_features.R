suppressMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(zoo)  # For rollmean/rollmax
})

# ==============================================================================
# Feature Engineering for PJM Load Forecasting
# Fixed to work with actual PJM data format from load_data.R
# ==============================================================================

create_modeling_features <- function(
    load_file = NULL,
    weather_file = NULL,
    output_file = "data/processed/modeling_features.rds",
    filter_timezone = "America/New_York") {
  
  # ===========================================================================
  # 1. Auto-detect PJM load files if not specified
  # ===========================================================================
  target_years <- c(2019, 2022, 2023, 2024)
  
  if (is.null(load_file)) {
    cat("Searching for PJM load files for years:", paste(target_years, collapse = ", "), "\n")
    
    # Look for year-specific files
    year_files <- c()
    for (year in target_years) {
      pattern <- sprintf("hrl_load_metered_%d\\.csv", year)
      year_file <- list.files("data/raw", pattern = pattern, full.names = TRUE, recursive = FALSE)
      if (length(year_file) > 0) {
        year_files <- c(year_files, year_file[1])
        cat("  Found:", basename(year_file[1]), "\n")
      }
    }
    
    if (length(year_files) == 0) {
      cat("  No year-specific files found. Looking for combined file...\n")
      
      # Look for any CSV with PJM data
      csv_files <- list.files("data/raw", pattern = "\\.csv$", full.names = TRUE, recursive = FALSE)
      csv_files <- csv_files[!grepl("weather", csv_files)]
      
      for (f in csv_files) {
        tryCatch({
          peek <- read_csv(f, n_max = 1, show_col_types = FALSE)
          if (("load_area" %in% names(peek) && "mw" %in% names(peek)) ||
              ("zone" %in% names(peek) && "load" %in% names(peek))) {
            load_file <- f
            cat("  Found combined file:", f, "\n")
            break
          }
        }, error = function(e) {})
      }
      
      if (is.null(load_file) && length(csv_files) > 0) {
        stop("Could not find PJM load file with correct columns.\n",
             "Available files in data/raw/: ", paste(basename(csv_files), collapse = ", "))
      } else if (is.null(load_file)) {
        stop("No CSV files found in data/raw/. Have you run 'Rscript load_data.R'?")
      }
    } else {
      # Use multiple year files
      load_file <- year_files
    }
  }
  
  # ===========================================================================
  # 2. Auto-detect weather file if not specified
  # ===========================================================================
  if (is.null(weather_file)) {
    cat("Searching for historical weather file...\n")
    
    # Look for the historical weather file from download_historical_weather.R
    weather_pattern <- "openmeteo_november_zone_.*\\.csv$"
    weather_dir <- "data/raw/weather/historical"
    
    if (dir.exists(weather_dir)) {
      weather_files <- list.files(weather_dir, pattern = weather_pattern, full.names = TRUE)
      
      if (length(weather_files) > 0) {
        # Use the first match (or most recent if multiple)
        weather_file <- weather_files[1]
        cat("Found weather file:", weather_file, "\n")
      }
    }
    
    if (is.null(weather_file)) {
      cat("WARNING: No weather file found at", weather_dir, "\n")
      cat("  Will proceed without weather features.\n")
      cat("  Run download_historical_weather.R to add weather data.\n\n")
    }
  }
  
  cat("\nLoading data...\n")
  if (length(load_file) == 1) {
    cat("  Load file:", load_file, "\n")
  } else {
    cat("  Load files (", length(load_file), "):", paste(basename(load_file), collapse = ", "), "\n")
  }
  if (!is.null(weather_file)) {
    cat("  Weather file:", weather_file, "\n")
  }
  cat("\n")
  
  # ===========================================================================
  # 3. Read and standardize PJM load data
  # ===========================================================================
  cat("Reading PJM load data...\n")
  
  # Handle single file or multiple files
  if (length(load_file) == 1) {
    if (!file.exists(load_file)) {
      stop("Load file not found: ", load_file)
    }
    load_data <- read_csv(load_file, show_col_types = FALSE)
  } else {
    # Multiple files - read and combine
    load_data_list <- list()
    for (f in load_file) {
      if (file.exists(f)) {
        cat("  Reading:", basename(f), "\n")
        load_data_list[[f]] <- read_csv(f, show_col_types = FALSE)
      } else {
        warning("File not found: ", f)
      }
    }
    load_data <- bind_rows(load_data_list)
  }
  
  cat("  Loaded", format(nrow(load_data), big.mark = ","), "rows\n")
  cat("  Columns:", paste(names(load_data), collapse = ", "), "\n")
  
  # Standardize column names
  # The PJM data has both 'zone' (broad) and 'load_area' (specific)
  # We want to use 'load_area' as our zone identifier (29 zones)
  
  # First, drop the broad 'zone' column if it exists
  if ("zone" %in% names(load_data) && "load_area" %in% names(load_data)) {
    load_data <- load_data |>
      select(-zone)  # Remove the broad zone column
  }
  
  # Now rename load_area to zone
  if ("load_area" %in% names(load_data)) {
    load_data <- load_data |>
      rename(zone = load_area)
  }
  
  # Rename mw to load
  if ("mw" %in% names(load_data) && !"load" %in% names(load_data)) {
    load_data <- load_data |>
      rename(load = mw)
  }
  
  # Standardize datetime column
  if ("datetime_beginning_ept" %in% names(load_data) && !"datetime" %in% names(load_data)) {
    load_data <- load_data |>
      rename(datetime = datetime_beginning_ept)
  }
  
  # Parse datetime and filter to November of target years
  cat("\nFiltering to November 2019, 2022, 2023, 2024...\n")
  load_data <- load_data |>
    mutate(
      datetime = mdy_hms(datetime, tz = filter_timezone),
      year = year(datetime),
      month = month(datetime)
    ) |>
    filter(
      year %in% c(2019, 2022, 2023, 2024),
      month == 11,
      zone != "RTO"  # Exclude RTO (aggregate zone, not one of the 29 specific zones)
    ) |>
    select(datetime, zone, load, year, month) |>
    # Remove any duplicates (keep first occurrence)
    distinct(datetime, zone, .keep_all = TRUE)
  
  cat("  Filtered to", format(nrow(load_data), big.mark = ","), "rows\n")
  cat("  Zones:", n_distinct(load_data$zone), "\n")
  cat("  Date range:", format(min(load_data$datetime)), "to", 
      format(max(load_data$datetime)), "\n\n")
  
  # ===========================================================================
  # 4. Read and join weather data (if available)
  # ===========================================================================
  has_weather <- FALSE
  
  if (!is.null(weather_file) && file.exists(weather_file)) {
    cat("Reading weather data...\n")
    weather_data <- read_csv(weather_file, show_col_types = FALSE)
    cat("  Loaded", format(nrow(weather_data), big.mark = ","), "rows\n")
    
    # Parse weather datetime (try multiple formats)
    weather_data <- weather_data |>
      mutate(
        time_ept = case_when(
          # Try standard format first: "2019-11-01 00:00:00"
          !is.na(ymd_hms(time_ept, tz = filter_timezone, quiet = TRUE)) ~ 
            ymd_hms(time_ept, tz = filter_timezone, quiet = TRUE),
          # Try without seconds: "2019-11-01 00:00"
          !is.na(ymd_hm(time_ept, tz = filter_timezone, quiet = TRUE)) ~ 
            ymd_hm(time_ept, tz = filter_timezone, quiet = TRUE),
          # Try US format: "11/1/2019 12:00:00 AM"
          !is.na(mdy_hms(time_ept, tz = filter_timezone, quiet = TRUE)) ~ 
            mdy_hms(time_ept, tz = filter_timezone, quiet = TRUE),
          # Already POSIXct (shouldn't happen but just in case)
          TRUE ~ as.POSIXct(time_ept, tz = filter_timezone)
        )
      ) |>
      # Remove duplicates in weather data
      distinct(load_area, time_ept, .keep_all = TRUE)
    
    # Join load and weather
    cat("Joining load and weather data...\n")
    modeling_data <- load_data |>
      left_join(
        weather_data,
        by = c("zone" = "load_area", "datetime" = "time_ept"),
        relationship = "many-to-many"  # Explicitly allow many-to-many if needed
      )
    
    cat("  Joined data has", format(nrow(modeling_data), big.mark = ","), "rows\n")
    
    # Check for weather columns
    weather_cols <- c("temp_mean", "CDH_mean", "HDH_mean")
    missing_weather <- weather_cols[!weather_cols %in% names(modeling_data)]
    
    if (length(missing_weather) == 0) {
      has_weather <- TRUE
      cat("  Weather features available: YES\n\n")
    } else {
      cat("  WARNING: Some weather columns missing:", paste(missing_weather, collapse = ", "), "\n\n")
    }
  } else {
    modeling_data <- load_data
    cat("Proceeding without weather data\n\n")
  }
  
  # ===========================================================================
  # 5. Create temporal features
  # ===========================================================================
  cat("Creating temporal features...\n")
  modeling_data <- modeling_data |>
    mutate(
      # Time features
      hour = hour(datetime),
      day_of_week = wday(datetime, week_start = 1),  # 1=Monday
      day_of_month = day(datetime),
      is_weekend = day_of_week %in% c(6, 7),
      
      # Holiday flag (Thanksgiving week - approximately Nov 22-28)
      is_holiday = (day_of_month >= 22 & day_of_month <= 28),
      
      # Hour categories
      hour_category = case_when(
        hour >= 0 & hour < 6 ~ "night",
        hour >= 6 & hour < 12 ~ "morning",
        hour >= 12 & hour < 18 ~ "afternoon",
        hour >= 18 & hour < 24 ~ "evening"
      )
    )
  
  # ===========================================================================
  # 6. Create lag features (by zone)
  # ===========================================================================
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
      load_roll_max_24 = rollmax(load, k = 24, fill = NA, align = "right")
    ) |>
    ungroup()
  
  # ===========================================================================
  # 7. Create weather-based lag features (if weather available)
  # ===========================================================================
  if (has_weather) {
    cat("Creating weather lag features...\n")
    modeling_data <- modeling_data |>
      arrange(zone, datetime) |>
      group_by(zone) |>
      mutate(
        # Temperature lags
        temp_lag1 = lag(temp_mean, 1),
        temp_lag24 = lag(temp_mean, 24)
      ) |>
      ungroup()
  }
  
  # ===========================================================================
  # 8. Create interaction features
  # ===========================================================================
  cat("Creating interaction features...\n")
  
  if (has_weather) {
    modeling_data <- modeling_data |>
      mutate(
        # Temperature-time interactions
        temp_hour = temp_mean * hour,
        CDH_hour = CDH_mean * hour,
        HDH_hour = HDH_mean * hour,
        
        # Weekend-temperature interaction
        temp_weekend = temp_mean * as.numeric(is_weekend)
      )
  }
  
  # ===========================================================================
  # 9. Add date identifier and zone statistics
  # ===========================================================================
  cat("Computing zone statistics...\n")
  
  modeling_data <- modeling_data |>
    mutate(
      date = as.Date(datetime),
      year_month = sprintf("%d-%02d", year, month)
    )
  
  # Create zone-specific statistics (for normalization)
  zone_stats <- modeling_data |>
    group_by(zone) |>
    summarize(
      load_mean = mean(load, na.rm = TRUE),
      load_sd = sd(load, na.rm = TRUE),
      load_min = ifelse(all(is.na(load)), NA_real_, min(load, na.rm = TRUE)),
      load_max = ifelse(all(is.na(load)), NA_real_, max(load, na.rm = TRUE)),
      .groups = "drop"
    ) |>
    filter(!is.na(load_mean))  # Remove zones with no valid data
  
  # Add zone statistics to data
  modeling_data <- modeling_data |>
    left_join(zone_stats, by = "zone")
  
  # ===========================================================================
  # 10. Save processed data
  # ===========================================================================
  cat("\nSaving processed data...\n")
  dir.create(dirname(output_file), showWarnings = FALSE, recursive = TRUE)
  
  saveRDS(list(
    data = modeling_data,
    zone_stats = zone_stats
  ), output_file)
  
  # ===========================================================================
  # 11. Print summary
  # ===========================================================================
  cat("\n==============================================================================\n")
  cat("Feature Engineering Complete\n")
  cat("==============================================================================\n")
  cat("Rows:", format(nrow(modeling_data), big.mark = ","), "\n")
  cat("Zones:", n_distinct(modeling_data$zone), "\n")
  cat("Date range:", format(min(modeling_data$datetime)), "to", format(max(modeling_data$datetime)), "\n")
  cat("Weather data:", ifelse(has_weather, "YES", "NO"), "\n")
  
  cat("\nMissing values in key columns:\n")
  key_cols <- c("load", "load_lag24", "load_lag168", "temp_mean", "load_roll_mean_24")
  for (col in key_cols) {
    if (col %in% names(modeling_data)) {
      n_missing <- sum(is.na(modeling_data[[col]]))
      pct_missing <- 100 * n_missing / nrow(modeling_data)
      cat(sprintf("  %-20s: %8d (%.1f%%)\n", col, n_missing, pct_missing))
    }
  }
  
  cat("\nZones present:\n")
  zone_counts <- modeling_data |>
    count(zone) |>
    arrange(zone)
  print(zone_counts, n = 50)
  
  cat("\nSaved to:", output_file, "\n")
  cat("==============================================================================\n")
  
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


