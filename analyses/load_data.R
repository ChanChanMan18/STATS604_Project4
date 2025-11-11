suppressMessages({
  library(readr); library(dplyr); library(httr2); library(fs); library(purrr)
  library(lubridate)
})

# ---- simple flag reader ----
args <- commandArgs(trailingOnly = TRUE)
kv <- function(flag, default = NA_character_) {
  i <- which(args == flag); if (length(i) && i < length(args)) args[i+1] else default
}

DATA_DIR <- kv("--data-dir", Sys.getenv("DATA_DIR", "data/raw"))
EXT_DIR  <- kv("--ext-dir",  Sys.getenv("EXT_DIR",  "data/external"))
OSF_URL  <- kv("--osf-url",
               Sys.getenv("PJM_OSF_ZIP_URL", "https://files.osf.io/v1/resources/Py3u6/providers/osfstorage/?zip="))
TZ_LOCAL <- kv("--tz",       Sys.getenv("PJM_TZ", "America/New_York"))
UA       <- Sys.getenv("NWS_USER_AGENT", "power-forecast/1.0")

# Anchors file path - construct it properly
ANCHORS  <- kv("--anchors", file.path(EXT_DIR, "pjm_airport_anchors.csv"))

# ---- 0) wipe raw dir ----
if (dir_exists(DATA_DIR)) suppressWarnings(dir_delete(DATA_DIR))
suppressMessages(dir_create(DATA_DIR, recurse = TRUE))

# ---- 1) download PJM load archive from OSF ----
zip_file <- tempfile(fileext = ".zip")
req <- request(OSF_URL) |>
  req_user_agent(UA) |>
  req_retry(max_tries = 5, backoff = ~ runif(1, 0.5, 1.5) * (2 ^ .x))
resp <- req_perform(req)
stopifnot(resp_status(resp) < 300)
writeBin(resp_body_raw(resp), zip_file)

# unzip into DATA_DIR (quiet)
utils::unzip(zip_file, exdir = DATA_DIR, overwrite = TRUE)
unlink(zip_file)

# extract any .tar.gz inside
tars <- dir(DATA_DIR, pattern = "\\.tar\\.gz$", full.names = TRUE, recursive = TRUE)
if (length(tars)) {
  walk(tars, ~ utils::untar(.x, exdir = DATA_DIR))
}

# ---- 2) Set environment variables for weather scripts ----
Sys.setenv(DATA_DIR = DATA_DIR)
Sys.setenv(PJM_TZ   = TZ_LOCAL)
Sys.setenv(NWS_USER_AGENT = UA)
Sys.setenv(EXT_DIR = EXT_DIR)

# Must exist
if (!file.exists(ANCHORS)) {
  stop("Anchors CSV not found at: ", ANCHORS, "\n",
       "  Expected location: ", file.path(EXT_DIR, "pjm_airport_anchors.csv"), "\n",
       "  EXT_DIR is: ", EXT_DIR, "\n",
       "  Please ensure the file exists at this location.",
       call. = FALSE)
}

# Set ANCHORS_CSV for child scripts (after verifying file exists)
Sys.setenv(ANCHORS_CSV = ANCHORS)

# ---- 3) Download historical weather data (Open-Meteo) ----
# November only for 2019, 2022, 2023, 2024 (excludes COVID years)
if (file.exists("analyses/download_historical_weather.R")) {
  source("analyses/download_historical_weather.R")
  invisible(download_historical_weather_november(
    anchors_csv = ANCHORS,
    years = c(2019, 2022, 2023, 2024),
    filter_timezone = TZ_LOCAL
  ))
}

# ---- 4) Download tomorrow's forecast (NWS) ----
source("analyses/download_weather_nws.R")
invisible(download_nws_from_anchors(
  anchors_csv = ANCHORS,
  filter_timezone = TZ_LOCAL,
  save_raw_json = TRUE
))
