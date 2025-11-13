suppressMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(purrr)
  library(readr)
})

# ==============================================================================
# PJM DataMiner JSON API — Hourly Load (Final Metered Only)
#   Window: 2024-11-03 (inclusive) --> yesterday (dynamic), in EPT
#   Feed:   hrl_load_metered
#
# Usage (interactive):
#   Sys.setenv(PJM_API_KEY="...", PJM_TZ="America/New_York")
#   source("analyses/download_pjm_load_current.R")
#   res <- download_pjm_load_current()
#
# CLI:
#   $env:PJM_API_KEY="..." ; $env:PJM_TZ="America/New_York" ; Rscript analyses/download_pjm_load_current.R
# ==============================================================================

# ---- Internal helper: fetch one window with paging ----
.pjm_fetch <- function(endpoint,
                       start_ept, end_ept,
                       fields,
                       api_key,
                       row_count = 50000,
                       verbose = TRUE,
                       user_agent = "pjm-loader-r/1.0") {
  stopifnot(nzchar(api_key))
  
  base <- paste0("https://api.pjm.com/api/v1/", endpoint)
  date_filter <- paste0(
    format(start_ept, "%m/%d/%Y %H:%M"),
    "to",
    format(end_ept,   "%m/%d/%Y %H:%M")
  )
  
  params <- list(
    fields = fields,
    sort = "datetime_beginning_utc",
    order = "Asc",
    startRow = 1,
    rowCount = row_count,
    datetime_beginning_ept = date_filter
  )
  
  url <- base
  out <- list()
  
  repeat {
    req <- request(url) |>
      req_url_query(!!!params) |>
      req_headers(
        "Accept" = "application/json",
        "Ocp-Apim-Subscription-Key" = api_key,
        "User-Agent" = user_agent
      ) |>
      req_timeout(120) |>
      req_retry(max_tries = 3, backoff = ~ 1 + runif(1, 0, 2)) |>
      req_error(is_error = ~ FALSE)
    
    resp <- req_perform(req)
    status <- resp_status(resp)
    if (status == 0L) { if (verbose) message("Connection error [", date_filter, "]"); break }
    if (status >= 400L) { if (verbose) message("HTTP ", status, " [", date_filter, "]"); break }
    
    j <- resp_body_json(resp, simplifyVector = TRUE)
    
    # --- items: can be a data.frame or a list of rows
    items <- j$items
    if (is.null(items) || length(items) == 0) break
    if (is.data.frame(items)) {
      out <- append(out, list(items))
    } else if (is.list(items)) {
      out <- append(out, list(dplyr::bind_rows(lapply(items, as.data.frame, stringsAsFactors = FALSE))))
    } else {
      break
    }
    
    # --- links: can be a data.frame (rel/href columns) or a list of lists
    next_href <- NULL
    links <- j$links
    if (!is.null(links)) {
      if (is.data.frame(links)) {
        idx <- which(tolower(links$rel) == "next")
        if (length(idx)) next_href <- links$href[[idx[1]]]
      } else if (is.list(links)) {
        for (lnk in links) {
          rel <- tryCatch(tolower(lnk$rel), error = function(e) NULL)
          if (!is.null(rel) && rel == "next") { next_href <- lnk$href; break }
        }
      }
    }
    
    if (is.null(next_href) || !nzchar(as.character(next_href))) break
    url <- as.character(next_href)
    params <- list() # 'next' already carries all params
  }
  
  if (!length(out)) return(tibble())
  dplyr::bind_rows(out)
}

# ---- Main: Nov 3, 2024 --> yesterday (EPT), metered only ----
download_pjm_load_current <- function(
    api_key = Sys.getenv("PJM_API_KEY", ""),
    filter_timezone = Sys.getenv("PJM_TZ", "America/New_York"),
    output_dir = "data/raw",
    days_per_call = 30,   # make each request cover this many days
    rpm = 6               # rate cap (non-member ~6/min) -> ~10s between calls
) {
  if (!nzchar(api_key)) stop("No PJM API key found. Set PJM_API_KEY or pass api_key=...")
  
  now_local <- with_tz(Sys.time(), filter_timezone)
  start_date <- as.Date("2025-11-03")            # fixed start (post-fallback)
  end_date   <- as.Date(now_local) - days(1)     # dynamic end (yesterday in EPT)
  if (end_date < start_date) stop("End date precedes start date (clock/timezone issue).")
  
  cat("==============================================================================\n")
  cat("PJM Load Data Download (Final Metered)\n")
  cat("==============================================================================\n")
  cat("Timezone:", filter_timezone, "\n")
  cat("Current local time:", format(now_local, "%Y-%m-%d %H:%M:%S %Z"), "\n")
  cat("Date range:", as.character(start_date), "to", as.character(end_date), "\n")
  cat("Chunk size (days):", days_per_call, " | Rate limit (rpm):", rpm, "\n")
  cat("==============================================================================\n\n")
  
  # Build chunk boundaries (inclusive start dates)
  chunk_starts <- seq(start_date, end_date, by = paste(days_per_call, "days"))
  n_chunks <- length(chunk_starts)
  gap_sec <- if (!is.null(rpm) && rpm > 0) 60 / rpm else 0
  
  fields_load <- paste(
    "datetime_beginning_ept",
    "datetime_beginning_utc",
    "load_area",
    "zone",
    "mw",
    "is_verified",
    sep = ","
  )
  
  cat(sprintf("Downloading in %d chunk(s)...\n", n_chunks))
  chunks <- vector("list", n_chunks)
  
  for (i in seq_along(chunk_starts)) {
    cs <- chunk_starts[i]
    ce <- min(cs + days(days_per_call) - days(1), end_date)  # inclusive chunk end date
    
    cat(sprintf("  [%d/%d] %s → %s ... ",
                i, n_chunks, format(cs, "%Y-%m-%d"), format(ce, "%Y-%m-%d")))
    
    start_ept <- as.POSIXct(paste(cs, "00:00"), tz = filter_timezone)
    end_ept   <- as.POSIXct(paste(ce + days(1), "00:00"), tz = filter_timezone)  # next midnight
    
    df_chunk <- .pjm_fetch(
      endpoint = "hrl_load_metered",
      start_ept = start_ept,
      end_ept   = end_ept,
      fields    = fields_load,
      api_key   = api_key,
      row_count = 50000,
      verbose   = FALSE
    )
    
    if (nrow(df_chunk)) {
      chunks[[i]] <- df_chunk
      cat(sprintf("OK (%d rows)\n", nrow(df_chunk)))
    } else {
      cat("NO DATA\n")
    }
    
    if (gap_sec > 0 && i < n_chunks) Sys.sleep(gap_sec)  # polite throttle
  }
  
  raw <- dplyr::bind_rows(chunks)
  if (!nrow(raw)) stop("No data retrieved. Check network/key/permissions.")
  
  # Parse & clean
  data <- raw |>
    dplyr::mutate(
      mw = suppressWarnings(as.numeric(mw)),
      is_verified = as.logical(is_verified),
      datetime_beginning_utc = lubridate::ymd_hms(datetime_beginning_utc, tz = "UTC"),
      # PJM returns EPT as ISO "YYYY-MM-DDTHH:MM:SS"
      datetime_beginning_ept = lubridate::ymd_hms(datetime_beginning_ept, tz = filter_timezone)
    ) |>
    dplyr::filter(!is.na(datetime_beginning_ept), !is.na(mw)) |>
    dplyr::distinct(datetime_beginning_ept, load_area, .keep_all = TRUE) |>
    dplyr::arrange(load_area, datetime_beginning_ept)
  
  cat("\nTotal rows after combine/dedup:", format(nrow(data), big.mark = ","), "\n")
  zones <- sort(unique(data$load_area))
  cat("Zones (", length(zones), "): ", paste(zones, collapse = ", "), "\n", sep = "")
  
  # DST-aware sanity check
  date_seq <- seq(start_date, end_date, by = "day")
  hours_per_day <- vapply(date_seq, function(d) {
    s <- as.POSIXct(paste(d, "00:00"), tz = filter_timezone)
    e <- as.POSIXct(paste(d + lubridate::days(1), "00:00"), tz = filter_timezone)
    as.integer(difftime(e, s, units = "hours"))
  }, integer(1))
  expected_rows <- sum(hours_per_day) * length(zones)
  actual_rows   <- nrow(data)
  cat("Expected rows (zones × hours): ", format(expected_rows, big.mark = ","), "\n", sep = "")
  cat("Actual rows:                   ", format(actual_rows,   big.mark = ","), "\n", sep = "")
  
  # Save
  out_dir <- file.path(output_dir, "load", "current")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  outfile <- file.path(
    out_dir,
    sprintf("pjm_load_%s_to_%s.csv",
            format(start_date, "%Y%m%d"),
            format(end_date,   "%Y%m%d"))
  )
  
  readr::write_csv(
    data |>
      dplyr::mutate(datetime_beginning_ept = format(datetime_beginning_ept, "%Y-%m-%d %H:%M:%S")),
    outfile
  )
  
  cat("\nSaved:", outfile, "\n")
  if (file.exists(outfile)) cat("File size:", round(file.info(outfile)$size / 1024^2, 2), "MB\n")
  
  invisible(list(data = data, output_file = outfile, zones = zones))
}