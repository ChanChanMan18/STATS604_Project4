get_script_dir <- function() {
  if (suppressWarnings(requireNamespace("rstudioapi", quietly = TRUE))) {
    if (isTRUE(tryCatch(rstudioapi::isAvailable(), error = function(e) FALSE))) {
      p <- tryCatch(rstudioapi::getActiveDocumentContext()$path, error = function(e) "")
      if (nzchar(p)) return(dirname(normalizePath(p)))
    }
  }
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg)) return(dirname(normalizePath(sub("^--file=", "", file_arg))))
  normalizePath(getwd())
}

script_dir <- get_script_dir()
raw_dir    <- normalizePath(file.path(script_dir, "..", "data", "raw"), mustWork = FALSE)

# Ensure ../data/raw exists
dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)

# Download OSF ZIP quietly to a temp file
zip_url  <- "https://files.osf.io/v1/resources/Py3u6/providers/osfstorage/?zip="
zip_file <- tempfile(fileext = ".zip")

ok <- try(utils::download.file(zip_url, zip_file, mode = "wb", method = "libcurl", quiet = TRUE), silent = TRUE)
if (inherits(ok, "try-error")) {
  utils::download.file(zip_url, zip_file, mode = "wb", quiet = TRUE)
}

# Unzip into ../data/raw quietly
suppressWarnings(utils::unzip(zip_file, exdir = raw_dir, overwrite = TRUE))
unlink(zip_file, force = TRUE)

# If OSF included a nested "raw/" directory, flatten it into ../data/raw
nested_raw <- file.path(raw_dir, "raw")
if (dir.exists(nested_raw)) {
  items <- list.files(nested_raw, all.files = TRUE, full.names = TRUE, no.. = TRUE)
  if (length(items)) {
    suppressWarnings(file.rename(items, file.path(raw_dir, basename(items))))
  }
  unlink(nested_raw, recursive = TRUE, force = TRUE)
}

# Find and extract any .tar.gz archives directly into ../data/raw, then remove them
tars <- list.files(raw_dir, pattern = "\\.tar\\.gz$", full.names = TRUE, recursive = TRUE)
if (length(tars)) {
  for (tp in tars) {
    suppressWarnings(utils::untar(tp, exdir = raw_dir))
    unlink(tp, force = TRUE)
  }
}

invisible(NULL)