suppressMessages({
  library(httr2); library(jsonlite); library(readr)
  library(dplyr); library(tidyr); library(lubridate); library(purrr)
})

# ==============================================================================
# Open-Meteo Forecast API - Tomorrow's Weather
# Replaces NWS download with faster Open-Meteo forecast
# ==============================================================================

download_weather_forecast <- function(
    anchors_csv,
    filter_timezone = Sys.getenv("PJM_TZ", "America/New_York"),
    run_tag = format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")) {
  
  # Get configuration from environment
  DATA_DIR <- Sys.getenv("DATA_DIR", "data/raw")
  UA <- Sys.getenv("NWS_USER_AGENT", "power-forecast/1.0")
  
  # Create weather directories
  WEATHER_DIR <- file.path(DATA_DIR, "weather")
  dir.create(file.path(WEATHER_DIR, "csv"), showWarnings = FALSE, recursive = TRUE)
  
  # Read and prepare anchors
  anchors <- suppressWarnings(readr::read_csv(anchors_csv, show_col_types = FALSE)) |>
    transmute(
      load_area = as.character(load_area),
      airport_code = as.character(airport_code),
      anchor = paste0(airport_code, " - ", as.character(airport_name)),
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    ) |>
    filter(!is.na(load_area), !is.na(lat), !is.na(lon))
  
  # Equal weights per load_area
  anchors <- anchors |>
    group_by(load_area) |>
    mutate(weight = 1.0 / n()) |>
    ungroup()
  
  # ===========================================================================
  # Define target time window: TOMORROW in Eastern Time (00:00 to 23:59)
  # ===========================================================================
  
  tz_ept <- filter_timezone  # e.g., "America/New_York"
  
  # Current time in Eastern
  now_ept <- with_tz(Sys.time(), tz_ept)
  
  # Tomorrow at midnight in Eastern (start of target day)
  target_start_ept <- floor_date(now_ept + days(1), unit = "day")
  
  # End of tomorrow (start of day after tomorrow)
  target_end_ept <- target_start_ept + days(1)
  
  # Convert to dates for API call
  tomorrow_date <- as.Date(target_start_ept)
  tomorrow_str <- as.character(tomorrow_date)
  
  cat("Downloading weather forecast for tomorrow:", tomorrow_str, "\n")
  cat("  Timezone:", tz_ept, "\n")
  cat("  Current time:", format(now_ept, "%Y-%m-%d %H:%M:%S %Z"), "\n\n")
  
  # ===========================================================================
  # Fetch weather forecast for each anchor
  # ===========================================================================
  
  cat("Fetching forecast for", nrow(anchors), "anchor points...\n")
  
  per_anchor <- purrr::map_dfr(seq_len(nrow(anchors)), function(i) {
    a <- anchors[i, ]
    
    # Open-Meteo Forecast API URL
    # Request 2 days to ensure we get all of tomorrow regardless of timezone
    url <- sprintf(
      "https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&timezone=UTC&forecast_days=2",
      a$lat, a$lon
    )
    
    # Make request with retry
    req <- request(url) |>
      req_user_agent(UA) |>
      req_retry(max_tries = 3, backoff = ~runif(1, 0.5, 1.5) * (2^.x))
    
    resp <- try(req_perform(req), silent = TRUE)
    if (inherits(resp, "try-error") || resp_status(resp) >= 400) {
      return(tibble())
    }
    
    json_resp <- resp_body_json(resp)
    
    # Check if we got valid data
    if (is.null(json_resp$hourly)) return(tibble())
    
    hourly <- json_resp$hourly
    
    # Convert list elements to vectors
    time_chars <- unlist(hourly$time)
    
    # Parse timestamps - Open-Meteo returns ISO8601 format in UTC
    time_utc <- as.POSIXct(time_chars, format = "%Y-%m-%dT%H:%M", tz = "UTC")
    
    # Parse and create data frame
    tibble(
      load_area = a$load_area,
      anchor = a$anchor,
      lat = a$lat,
      lon = a$lon,
      time_utc = time_utc,
      temp_F = as.numeric(unlist(hourly$temperature_2m)),
      relHum = as.numeric(unlist(hourly$relative_humidity_2m)),
      precip_in = as.numeric(unlist(hourly$precipitation)),
      wind_mph = as.numeric(unlist(hourly$wind_speed_10m)),
      wind_dir = as.numeric(unlist(hourly$wind_direction_10m))
    ) |>
      mutate(
        # Convert precipitation inches (NA to 0)
        precip_in = ifelse(is.na(precip_in), 0, precip_in),
        # Cooling Degree Hours (base 65°F)
        CDH = pmax(0, temp_F - 65),
        # Heating Degree Hours (base 65°F)
        HDH = pmax(0, 65 - temp_F),
        # Convert wind direction to cardinal direction
        windDir = case_when(
          is.na(wind_dir) ~ "CALM",
          wind_dir < 22.5 | wind_dir >= 337.5 ~ "N",
          wind_dir >= 22.5 & wind_dir < 67.5 ~ "NE",
          wind_dir >= 67.5 & wind_dir < 112.5 ~ "E",
          wind_dir >= 112.5 & wind_dir < 157.5 ~ "SE",
          wind_dir >= 157.5 & wind_dir < 202.5 ~ "S",
          wind_dir >= 202.5 & wind_dir < 247.5 ~ "SW",
          wind_dir >= 247.5 & wind_dir < 292.5 ~ "W",
          wind_dir >= 292.5 & wind_dir < 337.5 ~ "NW",
          TRUE ~ "CALM"
        )
      )
  }, .progress = FALSE)
  
  if (nrow(per_anchor) == 0) {
    stop("No forecast data retrieved. Check network connection and anchor coordinates.")
  }
  
  cat("Fetched", format(nrow(per_anchor), big.mark = ","), "hourly forecast observations\n")
  
  # ===========================================================================
  # Convert to Eastern time and filter to tomorrow only
  # ===========================================================================
  
  per_anchor <- per_anchor |>
    mutate(start_ept = with_tz(time_utc, tz_ept)) |>
    select(load_area, anchor, lat, lon, start_ept, time_utc, 
           temp_F, relHum, precip_in, wind_mph, windDir, CDH, HDH)
  
  # Filter to tomorrow's 24 hours in Eastern time
  per_anchor <- per_anchor |>
    filter(start_ept >= target_start_ept, start_ept < target_end_ept)
  
  cat("Filtered to", format(nrow(per_anchor), big.mark = ","), "observations for tomorrow\n")
  
  # ===========================================================================
  # Ensure exactly 24 hours per anchor with imputation if needed
  # ===========================================================================
  
  # Create sequence of 24 expected hours
  expected_24_ept <- target_start_ept + hours(0:23)
  
  # Imputation helper functions
  fill_linear <- function(x) {
    n <- length(x)
    idx <- which(!is.na(x))
    if (!length(idx)) return(rep(NA_real_, n))
    
    # Forward/backward fill first
    xcf <- x
    for (i in seq_len(n)) {
      if (is.na(xcf[i])) {
        xcf[i] <- if (i > 1) xcf[i - 1] else x[idx[1]]
      }
    }
    for (i in n:1) {
      if (is.na(xcf[i])) {
        xcf[i] <- if (i < n) xcf[i + 1] else x[idx[length(idx)]]
      }
    }
    
    # Linear interpolation
    xlin <- x
    last <- NA_integer_
    for (i in seq_len(n)) {
      if (!is.na(xlin[i])) {
        last <- i
        next
      }
      nxt <- if (any(!is.na(xlin[(i + 1):n]))) {
        i + which(!is.na(xlin[(i + 1):n]))[1]
      } else {
        NA_integer_
      }
      if (!is.na(last) && !is.na(nxt)) {
        frac <- (i - last) / (nxt - last)
        xlin[i] <- (1 - frac) * xlin[last] + frac * xlin[nxt]
      }
    }
    ifelse(is.na(xlin), xcf, xlin)
  }
  
  fill_carry <- function(x) {
    x2 <- x
    for (i in seq_along(x2)) {
      if (is.na(x2[i]) || x2[i] == "") {
        x2[i] <- if (i > 1) x2[i - 1] else x2[i]
      }
    }
    for (i in length(x2):1) {
      if (is.na(x2[i]) || x2[i] == "") {
        x2[i] <- if (i < length(x2)) x2[i + 1] else x2[i]
      }
    }
    x2
  }
  
  # Ensure exactly 24 hours per anchor
  per_anchor <- per_anchor |>
    group_by(load_area, anchor, lat, lon) |>
    complete(start_ept = expected_24_ept) |>
    arrange(load_area, anchor, start_ept) |>
    mutate(
      temp_F = fill_linear(temp_F),
      relHum = fill_linear(relHum),
      precip_in = fill_linear(precip_in),
      wind_mph = fill_linear(wind_mph),
      CDH = fill_linear(CDH),
      HDH = fill_linear(HDH),
      windDir = fill_carry(windDir)
    ) |>
    ungroup() |>
    filter(start_ept >= target_start_ept, start_ept < target_end_ept)
  
  # Add weights
  per_anchor <- per_anchor |>
    left_join(
      anchors |> select(load_area, anchor, weight),
      by = c("load_area", "anchor")
    )
  
  # ===========================================================================
  # Aggregate by zone and hour
  # ===========================================================================
  
  cat("Aggregating by zone and hour...\n")
  
  # Safe aggregation functions
  safe_max <- function(x) {
    if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
  }
  
  safe_range <- function(x) {
    if (all(is.na(x))) return(NA_real_)
    r <- range(x, na.rm = TRUE)
    r[2] - r[1]
  }
  
  safe_wmean <- function(x, w) {
    if (all(is.na(x))) NA_real_ else weighted.mean(x, w, na.rm = TRUE)
  }
  
  # Aggregate to zone-hour level
  zone_hour <- per_anchor |>
    group_by(load_area, start_ept) |>
    summarize(
      temp_mean = safe_wmean(temp_F, weight),
      temp_max = safe_max(temp_F),
      temp_range = safe_range(temp_F),
      rh_mean = safe_wmean(relHum, weight),
      precip_in = safe_max(precip_in),
      wind_mph = safe_wmean(wind_mph, weight),
      CDH_mean = safe_wmean(CDH, weight),
      HDH_mean = safe_wmean(HDH, weight),
      .groups = "drop"
    ) |>
    # Ensure exactly 24 hours per zone
    group_by(load_area) |>
    complete(start_ept = expected_24_ept) |>
    arrange(load_area, start_ept) |>
    # Impute any remaining missing values
    mutate(
      temp_mean = fill_linear(temp_mean),
      temp_max = fill_linear(temp_max),
      temp_range = fill_linear(temp_range),
      rh_mean = fill_linear(rh_mean),
      precip_in = fill_linear(precip_in),
      wind_mph = fill_linear(wind_mph),
      CDH_mean = fill_linear(CDH_mean),
      HDH_mean = fill_linear(HDH_mean)
    ) |>
    ungroup() |>
    filter(start_ept >= target_start_ept, start_ept < target_end_ept)
  
  # Add UTC column for reference
  per_anchor <- per_anchor |>
    mutate(start_utc = with_tz(start_ept, "UTC")) |>
    select(load_area, anchor, lat, lon, start_ept, start_utc, everything())
  
  zone_hour <- zone_hour |>
    mutate(start_utc = with_tz(start_ept, "UTC")) |>
    select(load_area, start_ept, start_utc, everything())
  
  cat("Aggregated to", format(nrow(zone_hour), big.mark = ","), "zone-hour records\n")
  
  # ===========================================================================
  # Write output files
  # ===========================================================================
  
  per_anchor_csv <- file.path(
    WEATHER_DIR, "csv",
    paste0("forecast_per_anchor_", run_tag, ".csv")
  )
  zone_hour_csv <- file.path(
    WEATHER_DIR, "csv",
    paste0("forecast_zone_", run_tag, ".csv")
  )
  
  # Format datetime columns to strings for CSV output
  per_anchor_out <- per_anchor |>
    mutate(
      start_ept = format(start_ept, "%Y-%m-%d %H:%M:%S"),
      start_utc = format(start_utc, "%Y-%m-%d %H:%M:%S")
    )
  
  zone_hour_out <- zone_hour |>
    mutate(
      start_ept = format(start_ept, "%Y-%m-%d %H:%M:%S"),
      start_utc = format(start_utc, "%Y-%m-%d %H:%M:%S")
    )
  
  suppressWarnings(write_csv(per_anchor_out, per_anchor_csv))
  suppressWarnings(write_csv(zone_hour_out, zone_hour_csv))
  
  cat("\n==============================================================================\n")
  cat("Weather forecast download complete!\n")
  cat("==============================================================================\n")
  cat("Per-anchor file:", per_anchor_csv, "\n")
  cat("Zone-hour file:", zone_hour_csv, "\n")
  cat("Forecast date:", tomorrow_str, "\n")
  cat("Total zones:", n_distinct(zone_hour$load_area), "\n")
  cat("==============================================================================\n")
  
  # Return paths (matching NWS script interface)
  invisible(list(
    per_anchor = per_anchor_csv,
    zone_hour = zone_hour_csv
  ))
}

# ==============================================================================
# If run directly
# ==============================================================================

if (!interactive()) {
  # Get configuration
  DATA_DIR <- Sys.getenv("DATA_DIR", "data/raw")
  ANCHORS <- Sys.getenv("ANCHORS_CSV", "data/external/pjm_airport_anchors.csv")
  TZ_LOCAL <- Sys.getenv("PJM_TZ", "America/New_York")
  
  # Set defaults
  Sys.setenv(DATA_DIR = DATA_DIR)
  Sys.setenv(PJM_TZ = TZ_LOCAL)
  
  if (!file.exists(ANCHORS)) {
    stop("Anchors CSV not found at: ", ANCHORS, call. = FALSE)
  }
  
  # Download tomorrow's forecast
  invisible(download_weather_forecast(
    anchors_csv = ANCHORS,
    filter_timezone = TZ_LOCAL
  ))
}