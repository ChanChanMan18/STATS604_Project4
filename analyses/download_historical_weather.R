suppressMessages({
  library(httr2); library(jsonlite); library(readr)
  library(dplyr); library(tidyr); library(lubridate); library(purrr)
})

# ==============================================================================
# Open-Meteo Historical Weather Download - November Only, Selected Years
# ==============================================================================

download_historical_weather_november <- function(
    anchors_csv,
    years = c(2019, 2022, 2023, 2024),
    filter_timezone = Sys.getenv("PJM_TZ", "America/New_York")) {
  
  # Get configuration from environment
  DATA_DIR <- Sys.getenv("DATA_DIR", "data/raw")
  UA <- Sys.getenv("NWS_USER_AGENT", "power-forecast/1.0")
  
  # Create weather directories
  WEATHER_DIR <- file.path(DATA_DIR, "weather")
  dir.create(file.path(WEATHER_DIR, "historical"), showWarnings = FALSE, recursive = TRUE)
  
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
  
  # Loop through each November period
  all_per_anchor <- list()
  
  for (year in years) {
    # November 1 to November 30
    start_date <- as.Date(sprintf("%d-11-01", year))
    end_date <- as.Date(sprintf("%d-11-30", year))
    
    start_str <- as.character(start_date)
    end_str <- as.character(end_date)
    
    # Fetch weather for each anchor
    per_anchor <- purrr::map_dfr(seq_len(nrow(anchors)), function(i) {
      a <- anchors[i, ]
      
      # Open-Meteo API URL
      url <- sprintf(
        "https://archive-api.open-meteo.com/v1/archive?latitude=%.4f&longitude=%.4f&start_date=%s&end_date=%s&hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&timezone=UTC",
        a$lat, a$lon, start_str, end_str
      )
      
      # Make request with retry
      req <- request(url) |>
        req_user_agent(UA) |>
        req_retry(max_tries = 3, backoff = ~runif(1, 1, 3) * (2^.x))
      
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
      
      # Parse timestamps - Open-Meteo returns ISO8601 format
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
    
    all_per_anchor[[as.character(year)]] <- per_anchor
  }
  
  # Combine all years
  per_anchor <- bind_rows(all_per_anchor)
  
  if (nrow(per_anchor) == 0) {
    return(invisible(list()))
  }
  
  # Convert to Eastern time
  tz_ept <- filter_timezone
  per_anchor <- per_anchor |>
    mutate(time_ept = with_tz(time_utc, tz_ept)) |>
    select(load_area, anchor, lat, lon, time_ept, time_utc, 
           temp_F, relHum, precip_in, wind_mph, windDir, CDH, HDH)
  
  # Add weights
  per_anchor <- per_anchor |>
    left_join(
      anchors |> select(load_area, anchor, weight),
      by = c("load_area", "anchor")
    )
  
  # Aggregate by zone and hour
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
  
  # Group by load_area AND time_ept
  zone_hour <- per_anchor |>
    group_by(load_area, time_ept) |>
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
    mutate(time_utc = with_tz(time_ept, "UTC")) |>
    select(load_area, time_ept, time_utc, everything())
  
  # Write output files
  years_str <- paste(years, collapse = "_")
  
  per_anchor_csv <- file.path(
    WEATHER_DIR, "historical",
    sprintf("openmeteo_november_per_anchor_%s.csv", years_str)
  )
  zone_hour_csv <- file.path(
    WEATHER_DIR, "historical",
    sprintf("openmeteo_november_zone_%s.csv", years_str)
  )
  
  # Format datetime columns to strings for CSV output
  per_anchor_out <- per_anchor |>
    mutate(
      time_ept = format(time_ept, "%Y-%m-%d %H:%M:%S"),
      time_utc = format(time_utc, "%Y-%m-%d %H:%M:%S")
    )
  
  zone_hour_out <- zone_hour |>
    mutate(
      time_ept = format(time_ept, "%Y-%m-%d %H:%M:%S"),
      time_utc = format(time_utc, "%Y-%m-%d %H:%M:%S")
    )
  
  suppressWarnings(write_csv(per_anchor_out, per_anchor_csv))
  suppressWarnings(write_csv(zone_hour_out, zone_hour_csv))
  
  invisible(list(
    per_anchor = per_anchor_csv,
    zone_hour = zone_hour_csv
  ))
}

# ==============================================================================
# If run directly, download November data for selected years
# ==============================================================================

if (!interactive() && identical(basename(sys.frame(1)$ofile), "download_historical_weather.R")) {
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
  
  # Download November data for 2019, 2022, 2023, 2024
  invisible(download_historical_weather_november(
    anchors_csv = ANCHORS,
    years = c(2019, 2022, 2023, 2024),
    filter_timezone = TZ_LOCAL
  ))
}


