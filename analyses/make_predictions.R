suppressMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(zoo)
  library(randomForest)
})

# ==============================================================================
# Make Predictions for Tomorrow
# This script:
# 1. Downloads current PJM load data (Nov 3, 2025 -> yesterday)
# 2. Downloads current historical weather (Nov 1, 2025 -> yesterday)
# 3. Downloads tomorrow's NWS forecast
# 4. Creates features for tomorrow
# 5. Loads trained models
# 6. Makes predictions
# 7. Outputs in required format
# ==============================================================================

make_predictions <- function(
    models_dir = "models",
    data_dir = "data/raw",
    ext_dir = "data/external",
    filter_timezone = "America/New_York",
    verbose = TRUE) {
  
  # Helper function to print to stderr (diagnostic output)
  diag <- function(...) {
    if (verbose) cat(..., file = stderr())
  }
  
  # Set environment variables for download scripts
  Sys.setenv(DATA_DIR = data_dir)
  Sys.setenv(EXT_DIR = ext_dir)
  Sys.setenv(PJM_TZ = filter_timezone)
  Sys.setenv(NWS_USER_AGENT = "power-forecast/1.0")
  Sys.setenv(ANCHORS_CSV = file.path(ext_dir, "pjm_airport_anchors.csv"))
  
  diag("==============================================================================\n")
  diag("PJM Load Forecasting - Making Predictions for Tomorrow\n")
  diag("==============================================================================\n")
  diag("Timezone:", filter_timezone, "\n")
  diag("Current time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n\n")
  
  # ===========================================================================
  # Step 1: Download Current PJM Load Data
  # ===========================================================================
  diag("Step 1: Downloading current PJM load data...\n")
  source("analyses/download_pjm_load_current.R")
  pjm_result <- download_pjm_load_current(
    filter_timezone = filter_timezone,
    output_dir = data_dir
  )
  diag("\n")
  
  # ===========================================================================
  # Step 2: Download Current Historical Weather
  # ===========================================================================
  diag("Step 2: Downloading current historical weather...\n")
  source("analyses/download_historical_weather_current.R")
  hist_weather_result <- download_historical_weather_november_current(
    anchors_csv = file.path(ext_dir, "pjm_airport_anchors.csv"),
    filter_timezone = filter_timezone
  )
  diag("\n")
  
  # ===========================================================================
  # Step 3: Download Tomorrow's Weather Forecast
  # ===========================================================================
  diag("Step 3: Downloading tomorrow's weather forecast...\n")
  source("analyses/download_weather_forecast.R")
  forecast_result <- download_weather_forecast(
    anchors_csv = file.path(ext_dir, "pjm_airport_anchors.csv"),
    filter_timezone = filter_timezone
  )
  diag("\n")
  
  # ===========================================================================
  # Step 4: Load and Prepare Data
  # ===========================================================================
  diag("Step 4: Loading and preparing data...\n")
  
  # Load current PJM load data
  current_load_file <- pjm_result$output_file
  load_data <- read_csv(current_load_file, show_col_types = FALSE)
  
  diag("  Loaded", format(nrow(load_data), big.mark = ","), "rows of load data\n")
  
  # Load current historical weather
  current_weather_file <- hist_weather_result$zone_hour
  hist_weather <- read_csv(current_weather_file, show_col_types = FALSE)
  
  diag("  Loaded", format(nrow(hist_weather), big.mark = ","), "rows of historical weather\n")
  
  # Load tomorrow's forecast
  forecast_weather_file <- forecast_result$zone_hour
  forecast_weather <- read_csv(forecast_weather_file, show_col_types = FALSE)
  
  diag("  Loaded", format(nrow(forecast_weather), big.mark = ","), "rows of forecast weather\n")
  
  # ===========================================================================
  # Step 5: Standardize and Merge Historical Data
  # ===========================================================================
  diag("\nStep 5: Preparing historical data for lag features...\n")
  
  # Standardize load data
  # Drop the broad 'zone' column if it exists
  if ("zone" %in% names(load_data)) {
    load_data <- load_data |> select(-zone)
  }
  
  load_data <- load_data |>
    mutate(
      datetime_beginning_ept = ymd_hms(datetime_beginning_ept, tz = filter_timezone)
    ) |>
    rename(
      datetime = datetime_beginning_ept,
      zone = load_area,
      load = mw
    ) |>
    filter(zone != "RTO") |>
    select(datetime, zone, load) |>
    distinct(datetime, zone, .keep_all = TRUE)
  
  # Standardize historical weather
  hist_weather <- hist_weather |>
    mutate(
      time_ept = ymd_hms(time_ept, tz = filter_timezone)
    ) |>
    rename(datetime = time_ept) |>
    select(-time_utc)
  
  # Merge load and historical weather
  historical_data <- load_data |>
    left_join(hist_weather, by = c("zone" = "load_area", "datetime")) |>
    arrange(zone, datetime)
  
  diag("  Historical data has", format(nrow(historical_data), big.mark = ","), "rows\n")
  diag("  Date range:", format(min(historical_data$datetime)), "to",
       format(max(historical_data$datetime)), "\n")
  
  # ===========================================================================
  # Step 6: Prepare Tomorrow's Data Template
  # ===========================================================================
  diag("\nStep 6: Preparing tomorrow's prediction template...\n")
  
  # Get tomorrow's date
  now_local <- with_tz(Sys.time(), filter_timezone)
  tomorrow <- floor_date(now_local + days(1), unit = "day")
  
  # Create 24-hour sequence for tomorrow
  tomorrow_hours <- tomorrow + hours(0:23)
  
  diag("  Tomorrow's date:", format(tomorrow, "%Y-%m-%d"), "\n")
  diag("  Predicting for:", format(tomorrow_hours[1]), "to", format(tomorrow_hours[24]), "\n")
  
  # Standardize forecast weather
  forecast_weather <- forecast_weather |>
    mutate(
      start_ept = ymd_hms(start_ept, tz = filter_timezone)
    ) |>
    rename(datetime = start_ept) |>
    select(-start_utc)
  
  # Get unique zones
  zones <- unique(load_data$zone)
  zones <- zones[zones != "RTO"]
  zones <- sort(zones)
  
  diag("  Zones:", length(zones), "\n")
  
  # Create template for tomorrow's predictions
  tomorrow_template <- expand_grid(
    zone = zones,
    datetime = tomorrow_hours
  )
  
  # Join with forecast weather
  tomorrow_data <- tomorrow_template |>
    left_join(forecast_weather, by = c("zone" = "load_area", "datetime"))
  
  diag("  Tomorrow's template has", format(nrow(tomorrow_data), big.mark = ","), "rows\n")
  
  # ===========================================================================
  # Step 7: Create Features for Tomorrow
  # ===========================================================================
  diag("\nStep 7: Creating features for tomorrow...\n")
  
  # Add temporal features
  tomorrow_data <- tomorrow_data |>
    mutate(
      hour = hour(datetime),
      day_of_week = wday(datetime, week_start = 1),
      day_of_month = day(datetime),
      is_weekend = day_of_week %in% c(6, 7),
      is_holiday = (day_of_month >= 22 & day_of_month <= 28),
      year = year(datetime),
      month = month(datetime)
    )
  
  # For each zone, compute lag features from historical data
  diag("  Computing lag features from historical data...\n")
  
  all_zones_features <- list()
  
  for (zone_name in zones) {
    # Get historical data for this zone
    zone_hist <- historical_data |>
      filter(zone == zone_name) |>
      arrange(datetime)
    
    # Get tomorrow's data for this zone
    zone_tomorrow <- tomorrow_data |>
      filter(zone == zone_name) |>
      arrange(datetime)
    
    # Check if we have enough historical data
    if (nrow(zone_hist) < 168) {
      cat("  WARNING: Zone", zone_name, "has only", nrow(zone_hist), "hours of historical data\n")
    }
    
    # For each hour tomorrow, compute lag features
    # NOTE: We only use load_lag168 (last week) because load_lag1 and load_lag24
    # require today's data which isn't available yet from PJM
    zone_tomorrow <- zone_tomorrow |>
      rowwise() |>
      mutate(
        # Load lag: same hour last week (168 hours ago)
        load_lag168 = {
          lag168_time <- datetime - hours(168)
          lag168_data <- zone_hist |> filter(datetime == lag168_time)
          if (nrow(lag168_data) > 0 && !is.na(lag168_data$load[1])) {
            lag168_data$load[1]
          } else {
            NA_real_
          }
        },
        # Weather lags from yesterday
        temp_lag1 = {
          lag1_time <- datetime - hours(1)
          lag1_data <- zone_hist |> filter(datetime == lag1_time)
          if (nrow(lag1_data) > 0 && !is.na(lag1_data$temp_mean[1])) {
            lag1_data$temp_mean[1]
          } else {
            NA_real_
          }
        },
        temp_lag24 = {
          lag24_time <- datetime - hours(24)
          lag24_data <- zone_hist |> filter(datetime == lag24_time)
          if (nrow(lag24_data) > 0 && !is.na(lag24_data$temp_mean[1])) {
            lag24_data$temp_mean[1]
          } else {
            NA_real_
          }
        }
      ) |>
      ungroup()
    
    # Compute rolling features from the last 24 hours of historical data
    last_24_hours <- zone_hist |>
      filter(!is.na(load)) |>
      slice_tail(n = 24)
    
    if (nrow(last_24_hours) >= 24) {
      roll_mean <- mean(last_24_hours$load, na.rm = TRUE)
      roll_max <- max(last_24_hours$load, na.rm = TRUE)
    } else if (nrow(last_24_hours) > 0) {
      # Use what we have if less than 24 hours
      roll_mean <- mean(last_24_hours$load, na.rm = TRUE)
      roll_max <- max(last_24_hours$load, na.rm = TRUE)
    } else {
      roll_mean <- NA_real_
      roll_max <- NA_real_
    }
    
    zone_tomorrow <- zone_tomorrow |>
      mutate(
        load_roll_mean_24 = roll_mean,
        load_roll_max_24 = roll_max
      )
    
    all_zones_features[[zone_name]] <- zone_tomorrow
  }
  
  # Combine all zones
  tomorrow_data <- bind_rows(all_zones_features)
  
  # Create interaction features
  tomorrow_data <- tomorrow_data |>
    mutate(
      temp_hour = temp_mean * hour,
      CDH_hour = CDH_mean * hour,
      HDH_hour = HDH_mean * hour,
      temp_weekend = temp_mean * as.numeric(is_weekend),
      # Add hour × day_of_week interaction
      hour_dow = hour * day_of_week
    )
  
  diag("  Features created for", format(nrow(tomorrow_data), big.mark = ","), "rows\n")
  
  # ===========================================================================
  # Step 8: Load Models and Make Predictions
  # ===========================================================================
  diag("\nStep 8: Loading models and making predictions...\n")
  
  # Load models
  hourly_models <- readRDS(file.path(models_dir, "hourly_load_models.rds"))
  peak_hour_models <- readRDS(file.path(models_dir, "peak_hour_models.rds"))
  peak_day_models <- readRDS(file.path(models_dir, "peak_day_models.rds"))
  
  diag("  Loaded", length(hourly_models), "hourly load models\n")
  diag("  Loaded", length(peak_hour_models), "peak hour models\n")
  diag("  Loaded", length(peak_day_models), "peak day models\n")
  
  # Make hourly load predictions
  diag("\n  Making hourly load predictions...\n")
  
  # Check for missing values in key features
  diag("  Checking for missing values in prediction data...\n")
  na_counts <- sapply(tomorrow_data, function(x) sum(is.na(x)))
  na_features <- na_counts[na_counts > 0]
  if (length(na_features) > 0) {
    cat("  WARNING: Found missing values in features:\n")
    for (feat in names(na_features)) {
      cat(sprintf("    %-30s: %d NAs\n", feat, na_features[feat]))
    }
  }
  
  all_predictions <- list()
  
  for (zone_name in zones) {
    if (!zone_name %in% names(hourly_models)) {
      warning("No model found for zone: ", zone_name)
      next
    }
    
    model_info <- hourly_models[[zone_name]]
    model <- model_info$model
    features <- model_info$features
    
    # Get tomorrow's data for this zone
    zone_data <- tomorrow_data |>
      filter(zone == zone_name) |>
      arrange(datetime)
    
    # Check which features are available and which have NAs
    pred_data <- zone_data |> select(all_of(features))
    
    # Impute missing values with reasonable defaults
    for (col in names(pred_data)) {
      if (any(is.na(pred_data[[col]]))) {
        if (col %in% c("load_lag168", "load_roll_mean_24", "load_roll_max_24")) {
          # For load lags/rolling features, use zone mean if available
          zone_mean <- model_info$zone_stats$load_mean
          pred_data[[col]][is.na(pred_data[[col]])] <- zone_mean
        } else if (col %in% c("temp_lag1", "temp_lag24")) {
          # For temp lags, use forward fill from current temp
          pred_data[[col]][is.na(pred_data[[col]])] <- pred_data$temp_mean[1]
        } else if (is.numeric(pred_data[[col]])) {
          # For other numeric features, use column median or 0
          pred_data[[col]][is.na(pred_data[[col]])] <- median(pred_data[[col]], na.rm = TRUE)
          if (is.na(pred_data[[col]][1])) pred_data[[col]][is.na(pred_data[[col]])] <- 0
        }
      }
    }
    
    # Make predictions
    predictions <- predict(model, pred_data)
    
    zone_data$predicted_load <- predictions
    all_predictions[[zone_name]] <- zone_data
  }
  
  predictions_df <- bind_rows(all_predictions)
  
  diag("  Made", format(nrow(predictions_df), big.mark = ","), "hourly predictions\n")
  
  # ===========================================================================
  # Step 9: Make Peak Hour Predictions
  # ===========================================================================
  diag("\n  Making peak hour predictions...\n")
  
  peak_hour_predictions <- list()
  
  for (zone_name in zones) {
    if (!zone_name %in% names(peak_hour_models)) {
      # Use simple approach: hour with max predicted load
      zone_preds <- predictions_df |> filter(zone == zone_name)
      peak_hour <- zone_preds$hour[which.max(zone_preds$predicted_load)]
      peak_hour_predictions[[zone_name]] <- peak_hour
      next
    }
    
    # Use trained model
    ph_model_info <- peak_hour_models[[zone_name]]
    ph_model <- ph_model_info$model
    ph_features <- ph_model_info$features
    
    # Aggregate tomorrow's data to daily level
    zone_daily <- predictions_df |>
      filter(zone == zone_name) |>
      summarize(
        mean_temp = mean(temp_mean, na.rm = TRUE),
        max_temp = max(temp_max, na.rm = TRUE),
        min_temp = min(temp_mean, na.rm = TRUE),
        mean_CDH = mean(CDH_mean, na.rm = TRUE),
        mean_HDH = mean(HDH_mean, na.rm = TRUE),
        day_of_week = first(day_of_week),
        is_weekend = first(is_weekend),
        is_holiday = first(is_holiday)
      )
    
    # Make prediction
    ph_pred <- predict(ph_model, zone_daily |> select(all_of(ph_features)))
    peak_hour_predictions[[zone_name]] <- as.numeric(as.character(ph_pred))
  }
  
  diag("  Made", length(peak_hour_predictions), "peak hour predictions\n")
  
  # ===========================================================================
  # Step 10: Make Peak Day Predictions
  # ===========================================================================
  diag("\n  Making peak day predictions...\n")
  
  peak_day_predictions <- list()
  
  for (zone_name in zones) {
    if (!zone_name %in% names(peak_day_models)) {
      # Conservative approach: predict 0 (not a peak day) by default
      peak_day_predictions[[zone_name]] <- 0
      next
    }
    
    # Use trained model
    pd_model_info <- peak_day_models[[zone_name]]
    pd_model <- pd_model_info$model
    pd_features <- pd_model_info$features
    
    # Aggregate tomorrow's data
    zone_daily <- predictions_df |>
      filter(zone == zone_name) |>
      summarize(
        max_load = max(predicted_load, na.rm = TRUE),
        mean_load = mean(predicted_load, na.rm = TRUE),
        mean_temp = mean(temp_mean, na.rm = TRUE),
        max_temp = max(temp_max, na.rm = TRUE),
        min_temp = min(temp_mean, na.rm = TRUE),
        mean_CDH = mean(CDH_mean, na.rm = TRUE),
        mean_HDH = mean(HDH_mean, na.rm = TRUE),
        day_of_week = first(day_of_week),
        is_weekend = first(is_weekend),
        is_holiday = first(is_holiday)
      )
    
    # Make prediction
    pd_pred <- predict(pd_model, zone_daily |> select(all_of(pd_features)))
    peak_day_predictions[[zone_name]] <- as.integer(as.character(pd_pred))
  }
  
  diag("  Made", length(peak_day_predictions), "peak day predictions\n")
  
  # ===========================================================================
  # Step 11: Format Output
  # ===========================================================================
  diag("\nStep 11: Formatting output...\n")
  
  # Get tomorrow's date string
  tomorrow_date_str <- format(tomorrow, "%Y-%m-%d")
  
  # Create output in required format
  # "YYYY-MM-DD", L1_00, L1_01, ..., L1_23, L2_00, ..., L29_23, PH_1, ..., PH_29, PD_1, ..., PD_29
  
  output_parts <- list()
  output_parts[[1]] <- tomorrow_date_str
  
  # Add hourly load predictions (29 zones × 24 hours)
  for (zone_name in zones) {
    zone_preds <- predictions_df |>
      filter(zone == zone_name) |>
      arrange(hour) |>
      pull(predicted_load)
    
    # Round to nearest integer
    zone_preds <- round(zone_preds)
    
    output_parts <- c(output_parts, as.list(zone_preds))
  }
  
  # Add peak hour predictions (29 zones)
  for (zone_name in zones) {
    output_parts <- c(output_parts, peak_hour_predictions[[zone_name]])
  }
  
  # Add peak day predictions (29 zones)
  for (zone_name in zones) {
    output_parts <- c(output_parts, peak_day_predictions[[zone_name]])
  }
  
  # Convert to single line CSV
  output_line <- paste(output_parts, collapse = ", ")
  
  # ===========================================================================
  # Step 12: Output Results
  # ===========================================================================
  diag("\n==============================================================================\n")
  diag("Predictions Complete for", tomorrow_date_str, "\n")
  diag("==============================================================================\n")
  diag("Total predictions:\n")
  diag("  Hourly loads:", length(zones) * 24, "\n")
  diag("  Peak hours:", length(zones), "\n")
  diag("  Peak days:", length(zones), "\n")
  diag("==============================================================================\n\n")
  
  # Output to stdout (only this line should appear in stdout)
  cat(output_line, "\n")
  
  invisible(list(
    predictions = predictions_df,
    peak_hours = peak_hour_predictions,
    peak_days = peak_day_predictions,
    output = output_line
  ))
}

# ==============================================================================
# If run directly
# ==============================================================================

if (!interactive()) {
  make_predictions()
}