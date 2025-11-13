suppressMessages({
  library(readr)
  library(dplyr)
  library(randomForest)
  library(lubridate)
})

# ==============================================================================
# Train Random Forest Models for PJM Load Forecasting
# ==============================================================================

train_models <- function(
    features_file = "data/processed/modeling_features.rds",
    output_dir = "models",
    n_trees = 100,
    mtry = NULL) {
  
  # Load feature data
  cat("Loading feature data...\n")
  features_list <- readRDS(features_file)
  modeling_data <- features_list$data
  zone_stats <- features_list$zone_stats
  
  # Remove rows with NA in key lag features (first hours of dataset)
  modeling_data <- modeling_data |>
    filter(!is.na(load_lag168))
  
  # Define feature columns for modeling
  feature_cols <- c(
    # Time features
    "hour", "day_of_week", "day_of_month", "is_weekend", "is_holiday",
    
    # Weather features
    "temp_mean", "temp_max", "temp_range", 
    "rh_mean", "precip_in", "wind_mph",
    "CDH_mean", "HDH_mean",
    
    # Lag features (removed load_lag1 and load_lag24 for prediction-time availability)
    "load_lag168",
    "load_roll_mean_24", "load_roll_max_24",
    "temp_lag1", "temp_lag24",
    
    # Interaction features
    "temp_hour", "CDH_hour", "HDH_hour", "temp_weekend", "hour_dow"
  )
  
  # Remove any features with all NAs
  feature_cols <- feature_cols[sapply(feature_cols, function(col) {
    !all(is.na(modeling_data[[col]]))
  })]
  
  cat("Using", length(feature_cols), "features for modeling\n")
  
  # Create output directory
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Get unique zones
  zones <- unique(modeling_data$zone)
  cat("Training models for", length(zones), "zones\n\n")
  
  # Store models and performance metrics
  models <- list()
  performance <- list()
  
  # Train one model per zone
  for (zone_name in zones) {
    cat("Training model for zone:", zone_name, "\n")
    
    # Filter data for this zone
    zone_data <- modeling_data |>
      filter(zone == zone_name) |>
      select(all_of(c("load", feature_cols))) |>
      na.omit()
    
    if (nrow(zone_data) < 100) {
      cat("  WARNING: Not enough data for", zone_name, "- skipping\n")
      next
    }
    
    # Split into train/test (use 2019, 2022, 2023 for training, 2024 for testing)
    zone_data_full <- modeling_data |>
      filter(zone == zone_name) |>
      na.omit()
    
    train_data <- zone_data_full |> filter(year %in% c(2019, 2022, 2023))
    test_data <- zone_data_full |> filter(year == 2024)
    
    # Prepare training data
    train_X <- train_data |> select(all_of(feature_cols))
    train_y <- train_data$load
    
    # Train Random Forest
    set.seed(42)
    
    # Set mtry (number of variables to try at each split)
    if (is.null(mtry)) {
      mtry_val <- floor(sqrt(length(feature_cols)))
    } else {
      mtry_val <- mtry
    }
    
    rf_model <- randomForest(
      x = train_X,
      y = train_y,
      ntree = n_trees,
      mtry = mtry_val,
      importance = TRUE,
      nodesize = 5
    )
    
    # Evaluate on test set
    if (nrow(test_data) > 0) {
      test_X <- test_data |> select(all_of(feature_cols))
      test_y <- test_data$load
      
      predictions <- predict(rf_model, test_X)
      
      # Calculate metrics
      mse <- mean((predictions - test_y)^2)
      rmse <- sqrt(mse)
      mae <- mean(abs(predictions - test_y))
      mape <- mean(abs((predictions - test_y) / test_y)) * 100
      
      cat("  RMSE:", round(rmse, 2), "MW\n")
      cat("  MAE:", round(mae, 2), "MW\n")
      cat("  MAPE:", round(mape, 2), "%\n")
      
      performance[[zone_name]] <- data.frame(
        zone = zone_name,
        rmse = rmse,
        mae = mae,
        mape = mape,
        train_n = nrow(train_data),
        test_n = nrow(test_data)
      )
    }
    
    # Store model
    models[[zone_name]] <- list(
      model = rf_model,
      features = feature_cols,
      zone_stats = zone_stats |> filter(zone == zone_name)
    )
    
    cat("\n")
  }
  
  # Combine performance metrics
  performance_df <- bind_rows(performance)
  
  # Print overall summary
  cat("=== Overall Performance Summary ===\n")
  cat("Average RMSE:", round(mean(performance_df$rmse, na.rm = TRUE), 2), "MW\n")
  cat("Average MAE:", round(mean(performance_df$mae, na.rm = TRUE), 2), "MW\n")
  cat("Average MAPE:", round(mean(performance_df$mape, na.rm = TRUE), 2), "%\n")
  
  # Save models
  model_file <- file.path(output_dir, "hourly_load_models.rds")
  saveRDS(models, model_file)
  cat("\nModels saved to:", model_file, "\n")
  
  # Save performance metrics
  perf_file <- file.path(output_dir, "model_performance.csv")
  write_csv(performance_df, perf_file)
  cat("Performance metrics saved to:", perf_file, "\n")
  
  invisible(list(
    models = models,
    performance = performance_df
  ))
}

# ==============================================================================
# If run directly
# ==============================================================================

if (!interactive()) {
  train_models(n_trees = 100)
}