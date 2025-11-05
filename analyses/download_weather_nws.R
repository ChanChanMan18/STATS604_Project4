suppressMessages({
  library(httr2); library(jsonlite); library(readr)
  library(dplyr); library(tidyr); library(lubridate); library(purrr)
})

# ==============================================================================
# NWS API Helper Functions
# ==============================================================================

nws_gridpoint <- function(lat, lon, user_agent) {
  url <- sprintf("https://api.weather.gov/points/%.4f,%.4f", lat, lon)
  req <- request(url) |>
    req_user_agent(user_agent) |>
    req_retry(max_tries = 3, backoff = ~runif(1, 0.5, 1.5) * (2^.x))
  
  resp <- try(req_perform(req), silent = TRUE)
  if (inherits(resp, "try-error") || resp_status(resp) >= 400) return(NULL)
  
  content <- resp_body_json(resp)
  if (is.null(content$properties)) return(NULL)
  
  list(
    gridId = content$properties$gridId,
    gridX = content$properties$gridX,
    gridY = content$properties$gridY
  )
}

nws_hourly_forecast <- function(gridId, gridX, gridY, user_agent) {
  url <- sprintf("https://api.weather.gov/gridpoints/%s/%d,%d/forecast/hourly",
                 gridId, gridX, gridY)
  req <- request(url) |>
    req_user_agent(user_agent) |>
    req_retry(max_tries = 3, backoff = ~runif(1, 0.5, 1.5) * (2^.x))
  
  resp <- try(req_perform(req), silent = TRUE)
  if (inherits(resp, "try-error") || resp_status(resp) >= 400) return(NULL)
  
  resp_body_json(resp)
}

# Extract and normalize periods from NWS JSON response
normalize_periods <- function(json_response, load_area, anchor, lat, lon) {
  if (is.null(json_response$properties$periods)) return(tibble())
  
  periods <- json_response$properties$periods
  
  tibble(
    load_area = load_area,
    anchor = anchor,
    lat = lat,
    lon = lon,
    start_utc = lubridate::ymd_hms(sapply(periods, function(p) p$startTime)),
    temp_F = sapply(periods, function(p) as.numeric(p$temperature)),
    relHum = sapply(periods, function(p) as.numeric(p$relativeHumidity$value)),
    precip_p = sapply(periods, function(p) {
      pp <- p$probabilityOfPrecipitation$value
      if (is.null(pp)) 0 else as.numeric(pp)
    }),
    windDir = sapply(periods, function(p) {
      wd <- p$windDirection
      if (is.null(wd) || wd == "") "CALM" else wd
    }),
    shortFx = sapply(periods, function(p) {
      sf <- p$shortForecast
      if (is.null(sf)) "" else sf
    }),
    wind_mph = sapply(periods, function(p) {
      ws <- p$windSpeed
      if (is.null(ws) || ws == "") return(0)
      # Parse "X mph" or "X to Y mph"
      nums <- as.numeric(unlist(regmatches(ws, gregexpr("[0-9]+", ws))))
      if (length(nums) == 0) return(0)
      mean(nums)
    })
  ) |>
    mutate(
      # Cooling Degree Hours (base 65°F)
      CDH = pmax(0, temp_F - 65),
      # Heating Degree Hours (base 65°F)
      HDH = pmax(0, 65 - temp_F)
    )
}

# ==============================================================================
# Main Download Function
# ==============================================================================

download_nws_from_anchors <- function(
    anchors_csv,
    filter_timezone = Sys.getenv("PJM_TZ", "America/New_York"),
    save_raw_json = TRUE,
    run_tag = format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")) {
  
  # Get configuration from environment
  DATA_DIR <- Sys.getenv("DATA_DIR", "data/raw")
  UA <- Sys.getenv("NWS_USER_AGENT", "power-forecast/1.0 (contact@example.com)")
  
  # Create weather directories
  WEATHER_DIR <- file.path(DATA_DIR, "weather")
  dir.create(file.path(WEATHER_DIR, "json"), showWarnings = FALSE, recursive = TRUE)
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
  
  # Fetch weather data for each anchor
  per_anchor <- purrr::map_dfr(seq_len(nrow(anchors)), function(i) {
    a <- anchors[i, ]
    
    # Get grid point
    gp <- try(nws_gridpoint(a$lat, a$lon, UA), silent = TRUE)
    if (inherits(gp, "try-error") || is.null(gp$gridId)) return(tibble())
    
    # Get hourly forecast
    json_resp <- try(nws_hourly_forecast(gp$gridId, gp$gridX, gp$gridY, UA), silent = TRUE)
    if (inherits(json_resp, "try-error") || is.null(json_resp)) return(tibble())
    
    # Save raw JSON if requested
    if (isTRUE(save_raw_json)) {
      json_filename <- paste0(
        a$load_area, "_",
        gsub("[^A-Za-z0-9]+", "_", a$anchor), "_",
        run_tag, ".json"
      )
      json_path <- file.path(WEATHER_DIR, "json", json_filename)
      suppressWarnings(write_json(json_resp, json_path, auto_unbox = TRUE))
    }
    
    # Normalize and return
    normalize_periods(json_resp, a$load_area, a$anchor, a$lat, a$lon)
  })
  
  if (nrow(per_anchor) == 0) {
    return(invisible(list()))
  }
  
  # ==============================================================================
  # Define target time window: TOMORROW in Eastern Time (00:00 to 23:59)
  # ==============================================================================
  
  tz_ept <- filter_timezone  # e.g., "America/New_York"
  
  # Current time in Eastern
  now_ept <- with_tz(Sys.time(), tz_ept)
  
  # Tomorrow at midnight in Eastern (start of target day)
  target_start_ept <- floor_date(now_ept + days(1), unit = "day")
  
  # End of tomorrow (start of day after tomorrow)
  target_end_ept <- target_start_ept + days(1)
  
  # Create sequence of 24 expected hours (in Eastern time)
  expected_24_ept <- target_start_ept + hours(0:23)
  
  # ==============================================================================
  # Imputation helper functions
  # ==============================================================================
  
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
  
  # ==============================================================================
  # Process per-anchor data: filter to target window and ensure 24 hours
  # ==============================================================================
  
  per_anchor <- per_anchor |>
    # Convert UTC to Eastern
    mutate(start_ept = with_tz(start_utc, tz_ept)) |>
    # Filter to target day
    filter(start_ept >= target_start_ept, start_ept < target_end_ept) |>
    # Ensure exactly 24 hours per anchor
    group_by(load_area, anchor, lat, lon) |>
    complete(start_ept = expected_24_ept) |>
    arrange(load_area, anchor, start_ept) |>
    # Impute missing values
    mutate(
      temp_F = fill_linear(temp_F),
      relHum = fill_linear(relHum),
      precip_p = fill_linear(precip_p),
      wind_mph = fill_linear(wind_mph),
      CDH = fill_linear(CDH),
      HDH = fill_linear(HDH),
      windDir = fill_carry(windDir),
      shortFx = fill_carry(shortFx)
    ) |>
    ungroup() |>
    # Final filter to ensure no spillover
    filter(start_ept >= target_start_ept, start_ept < target_end_ept)
  
  # ==============================================================================
  # Add weights and aggregate by zone
  # ==============================================================================
  
  per_anchor <- per_anchor |>
    left_join(
      anchors |> select(load_area, anchor, weight),
      by = c("load_area", "anchor")
    )
  
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
      precip_p = safe_max(precip_p),
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
      precip_p = fill_linear(precip_p),
      wind_mph = fill_linear(wind_mph),
      CDH_mean = fill_linear(CDH_mean),
      HDH_mean = fill_linear(HDH_mean)
    ) |>
    ungroup() |>
    # Final filter
    filter(start_ept >= target_start_ept, start_ept < target_end_ept)
  
  # ==============================================================================
  # Add UTC column for reference (keeping EPT as primary)
  # ==============================================================================
  
  per_anchor <- per_anchor |>
    mutate(start_utc = with_tz(start_ept, "UTC")) |>
    select(load_area, anchor, lat, lon, start_ept, start_utc, everything())
  
  zone_hour <- zone_hour |>
    mutate(start_utc = with_tz(start_ept, "UTC")) |>
    select(load_area, start_ept, start_utc, everything())
  
  # ==============================================================================
  # Write output files (format datetime columns to preserve timezone)
  # ==============================================================================
  
  per_anchor_csv <- file.path(
    WEATHER_DIR, "csv",
    paste0("nws_hourly_per_anchor_", run_tag, ".csv")
  )
  zone_hour_csv <- file.path(
    WEATHER_DIR, "csv",
    paste0("nws_hourly_zone_", run_tag, ".csv")
  )
  
  # Format datetime columns to show timezone
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
  
  # Validation (silent)
  invisible(list(
    per_anchor = per_anchor_csv,
    zone_hour = zone_hour_csv
  ))
}