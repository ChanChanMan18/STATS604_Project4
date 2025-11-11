suppressMessages({
  library(readr)
  library(dplyr)
  library(randomForest)
})

# ==============================================================================
# Train Peak Hour and Peak Day Models
# ==============================================================================

train_peak_models <- function(
    features_file = "data/processed/modeling_features.rds",
    hourly_models_file = "data/processed/models/hourly_load_models.rds",
    output_dir = "data/processed/models") {
  
  cat("Loading data...\n")
  features_list <- readRDS(features_file)
  modeling_data <- features_list$data
  
  # Load hourly models (we'll use them for peak hour prediction)
  hourly_models <- readRDS(hourly_models_file)
  
  # ===========================================================================
  # TASK 2: Peak Hour Prediction
  # ===========================================================================
  cat("\n=== Training Peak Hour Models ===\n")
  
  # Create daily aggregates with peak hour labels
  daily_data <- modeling_data |>
    filter(!is.na(load)) |>
    group_by(zone, date, year) |>
    summarize(
      peak_hour = hour[which.max(load)],
      max_load = max(load, na.rm = TRUE),
      mean_load = mean(load, na.rm = TRUE),
      mean_temp = mean(temp_mean, na.rm = TRUE),
      max_temp = max(temp_max, na.rm = TRUE),
      min_temp = min(temp_mean, na.rm = TRUE),
      mean_CDH = mean(CDH_mean, na.rm = TRUE),
      mean_HDH = mean(HDH_mean, na.rm = TRUE),
      day_of_week = first(day_of_week),
      is_weekend = first(is_weekend),
      is_holiday = first(is_holiday),
      .groups = "drop"
    )
  
  peak_hour_models <- list()
  
  zones <- unique(daily_data$zone)
  
  for (zone_name in zones) {
    cat("Training peak hour model for:", zone_name, "\n")
    
    zone_daily <- daily_data |>
      filter(zone == zone_name) |>
      na.omit()
    
    if (nrow(zone_daily) < 20) {
      cat("  Not enough data - skipping\n")
      next
    }
    
    # Split train/test
    train_daily <- zone_daily |> filter(year %in% c(2019, 2022, 2023))
    test_daily <- zone_daily |> filter(year == 2024)
    
    # Features for peak hour prediction
    peak_hour_features <- c(
      "mean_temp", "max_temp", "min_temp",
      "mean_CDH", "mean_HDH",
      "day_of_week", "is_weekend", "is_holiday"
    )
    
    # Train model
    set.seed(42)
    ph_model <- randomForest(
      x = train_daily |> select(all_of(peak_hour_features)),
      y = as.factor(train_daily$peak_hour),
      ntree = 100,
      mtry = 3
    )
    
    # Evaluate
    if (nrow(test_daily) > 0) {
      pred_peak_hour <- predict(ph_model, test_daily |> select(all_of(peak_hour_features)))
      
      # Success = within ±1 hour
      accuracy <- mean(abs(as.numeric(as.character(pred_peak_hour)) - test_daily$peak_hour) <= 1)
      cat("  Accuracy (±1 hour):", round(accuracy * 100, 1), "%\n")
    }
    
    peak_hour_models[[zone_name]] <- list(
      model = ph_model,
      features = peak_hour_features
    )
  }
  
  # Save peak hour models
  ph_file <- file.path(output_dir, "peak_hour_models.rds")
  saveRDS(peak_hour_models, ph_file)
  cat("\nPeak hour models saved to:", ph_file, "\n")
  
  # ===========================================================================
  # TASK 3: Peak Day Prediction
  # ===========================================================================
  cat("\n=== Training Peak Day Models ===\n")
  
  # For each zone and year, identify the 2 peak days
  peak_days_labeled <- daily_data |>
    group_by(zone, year) |>
    mutate(
      rank_in_year = rank(-max_load, ties.method = "first"),
      is_peak_day = as.integer(rank_in_year <= 2)
    ) |>
    ungroup()
  
  peak_day_models <- list()
  
  for (zone_name in zones) {
    cat("Training peak day model for:", zone_name, "\n")
    
    zone_peak <- peak_days_labeled |>
      filter(zone == zone_name) |>
      na.omit()
    
    if (nrow(zone_peak) < 20) {
      cat("  Not enough data - skipping\n")
      next
    }
    
    # Split train/test
    train_peak <- zone_peak |> filter(year %in% c(2019, 2022, 2023))
    test_peak <- zone_peak |> filter(year == 2024)
    
    # Features for peak day prediction
    peak_day_features <- c(
      "max_load", "mean_load",
      "mean_temp", "max_temp", "min_temp",
      "mean_CDH", "mean_HDH",
      "day_of_week", "is_weekend", "is_holiday"
    )
    
    # Train model (weighted to penalize false negatives)
    set.seed(42)
    
    # Create class weights (penalize missing peak days more)
    class_weights <- ifelse(train_peak$is_peak_day == 1, 4, 1)
    
    pd_model <- randomForest(
      x = train_peak |> select(all_of(peak_day_features)),
      y = as.factor(train_peak$is_peak_day),
      ntree = 100,
      mtry = 3,
      classwt = c("0" = 1, "1" = 4)  # Weight peak days more heavily
    )
    
    # Evaluate
    if (nrow(test_peak) > 0) {
      pred_peak_day <- predict(pd_model, test_peak |> select(all_of(peak_day_features)))
      
      # Calculate custom loss
      actual <- test_peak$is_peak_day
      predicted <- as.integer(as.character(pred_peak_day))
      
      loss <- sum((actual == 1 & predicted == 0) * 4 +  # False negative
                    (actual == 0 & predicted == 1) * 1)   # False positive
      
      cat("  Custom loss:", loss, "\n")
      cat("  Accuracy:", round(mean(predicted == actual) * 100, 1), "%\n")
    }
    
    peak_day_models[[zone_name]] <- list(
      model = pd_model,
      features = peak_day_features
    )
  }
  
  # Save peak day models
  pd_file <- file.path(output_dir, "peak_day_models.rds")
  saveRDS(peak_day_models, pd_file)
  cat("\nPeak day models saved to:", pd_file, "\n")
  
  invisible(list(
    peak_hour_models = peak_hour_models,
    peak_day_models = peak_day_models
  ))
}

# ==============================================================================
# If run directly
# ==============================================================================

if (!interactive()) {
  train_peak_models()
}
