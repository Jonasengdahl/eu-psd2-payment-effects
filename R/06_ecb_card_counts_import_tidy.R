# scripts/06_ecb_card_counts_import_tidy.R
# =========================================
# Import & tidy ECB card payments counts (EU27, annual) — WIDE EXPORT
# Objective:
#   - Read raw ECB card payments transactions (counts) in WIDE format
#   - Filter to intended series definition (avoid mixing multiple CP series)
#   - Tidy into a clean country-year panel (long format)
#   - Save intermediate outputs + diagnostics + logs
#
# Inputs:
#   data/raw/ecb/card_payments/counts/ECB_cp_counts_EU27_annual_raw.csv
#
# Outputs:
#   data/intermediate/ecb/card_payments/cp_counts_country_year_intermediate.csv
#   data/intermediate/ecb/card_payments/cp_counts_country_year_intermediate.rds
#   outputs/tables/cp_counts_missingness_by_country.csv
#   outputs/tables/cp_counts_country_coverage.csv
#   outputs/tables/cp_counts_series_metadata.txt
#   outputs/logs/06_ecb_card_counts_import_tidy_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "06_ecb_card_counts_import_tidy_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("06_ecb_card_counts_import_tidy.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Define paths
# =========================================
raw_file <- here::here(
  "data/raw/ecb/card_payments/counts",
  "ECB_cp_counts_EU27_annual_raw.csv"
)

out_dir <- here::here("data/intermediate/ecb/card_payments")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "cp_counts_country_year_intermediate.csv")
out_rds <- file.path(out_dir, "cp_counts_country_year_intermediate.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

miss_country_csv <- file.path(diag_dir, "cp_counts_missingness_by_country.csv")
coverage_year_csv <- file.path(diag_dir, "cp_counts_country_coverage.csv")
meta_txt <- file.path(diag_dir, "cp_counts_series_metadata.txt")

stopifnot(file.exists(raw_file))

cat("Input file:\n  ", raw_file, "\n\n")
cat("Outputs:\n")
cat("  ", out_csv, "\n")
cat("  ", out_rds, "\n")
cat("  ", miss_country_csv, "\n")
cat("  ", coverage_year_csv, "\n")
cat("  ", meta_txt, "\n")
cat("  ", log_file, "\n\n")

# =========================================
# 2. Import raw ECB data (wide)
# =========================================
print_section("2. Import raw ECB data (wide)")

raw <- readr::read_csv(
  raw_file,
  show_col_types = FALSE,
  progress = FALSE,
  locale = readr::locale(encoding = "UTF-8"),
  col_types = readr::cols(.default = readr::col_character()),
  trim_ws = TRUE
)

# Capture readr/vroom parsing issues (if any)
p <- readr::problems(raw)
if (nrow(p) > 0) {
  cat("Parsing issues detected (showing first 20):\n")
  print(head(p, 20))
  cat("\n")
}

cat("Raw data dimensions: ", nrow(raw), " rows × ", ncol(raw), " cols\n\n")
cat("Raw columns (first 10):\n")
print(head(names(raw), 10))
cat("\n")

# =========================================
# 3. Identify time column (YEAR)
# =========================================
print_section("3. Identify time column (YEAR)")

preferred_time_cols <- c("TIME_PERIOD", "TIME PERIOD", "YEAR", "Year", "DATE", "Time period")
time_col <- preferred_time_cols[preferred_time_cols %in% names(raw)][1]

if (is.na(time_col)) {
  stop("Could not find a time column. Columns are: ", paste(names(raw), collapse = ", "))
}

cat("Detected time column: ", time_col, "\n\n")

raw <- raw |>
  dplyr::mutate(
    year = suppressWarnings(as.integer(stringr::str_extract(.data[[time_col]], "\\d{4}")))
  )

if (any(is.na(raw$year))) {
  cat("WARNING: Some years could not be parsed. Rows with NA year:\n")
  print(raw |>
          dplyr::filter(is.na(year)) |>
          dplyr::select(dplyr::all_of(time_col)))
  cat("\n")
}

cat("Year span (parsed): ", min(raw$year, na.rm = TRUE), "–", max(raw$year, na.rm = TRUE), "\n\n")

# =========================================
# 4. Detect and FILTER series columns (avoid mixing definitions)
# =========================================
print_section("4. Detect and FILTER series columns")

drop_cols <- unique(c(time_col, "DATE", "year"))
drop_cols <- drop_cols[drop_cols %in% names(raw)]
series_cols_all <- setdiff(names(raw), drop_cols)

cat("Series columns detected BEFORE filtering: ", length(series_cols_all), "\n\n")

series_cols <- series_cols_all

series_cols <- series_cols_all

# Filter 1: keep annual CP series columns by key prefix (PCP.A.)
# This avoids relying on human-readable labels that vary by country (e.g., DK/SE).
if (any(stringr::str_detect(series_cols, "PCP\\.A\\."))) {
  series_cols <- series_cols[stringr::str_detect(series_cols, "PCP\\.A\\.")]
}

# Filter 2: keep PN unit marker for counts (prefer the "(...PN)" style if present)
if (any(stringr::str_detect(series_cols, "\\.PN\\)"))) {
  series_cols <- series_cols[stringr::str_detect(series_cols, "\\.PN\\)")]
} else if (any(stringr::str_detect(series_cols, "\\.PN$"))) {
  series_cols <- series_cols[stringr::str_detect(series_cols, "\\.PN$")]
}

# Filter 3 (recommended if present): keep PN unit marker for counts
if (any(stringr::str_detect(series_cols, "\\.PN\\)"))) {
  series_cols <- series_cols[stringr::str_detect(series_cols, "\\.PN\\)")]
} else if (any(stringr::str_detect(series_cols, "\\.PN$"))) {
  series_cols <- series_cols[stringr::str_detect(series_cols, "\\.PN$")]
}

cat("Series columns AFTER filtering: ", length(series_cols), "\n\n")

if (length(series_cols) == 0) {
  stop("No series columns remain after filtering. Inspect the raw column names and adjust filters.")
}

meta_lines <- c(
  paste0("File: ", raw_file),
  paste0("Detected time column: ", time_col),
  paste0("Series columns BEFORE filtering: ", length(series_cols_all)),
  paste0("Series columns AFTER filtering: ", length(series_cols)),
  "",
  "First 10 series column names used:",
  paste0(" - ", head(series_cols, 10))
)
writeLines(meta_lines, meta_txt)

# =========================================
# 5. Pivot to long format
# =========================================
print_section("5. Pivot to long format")

cp_long <- raw |>
  dplyr::select(year, dplyr::all_of(series_cols)) |>
  tidyr::pivot_longer(
    cols = -year,
    names_to = "series_name",
    values_to = "value_raw"
  )

cat("Long data dimensions: ", nrow(cp_long), " rows × ", ncol(cp_long), " cols\n\n")

# =========================================
# 6. Extract country ISO2 from series key (...A.SE....)
# =========================================
print_section("6. Extract country code from series key")

cp_long <- cp_long |>
  dplyr::mutate(
    country = stringr::str_match(series_name, "\\.A\\.([A-Z]{2})\\.")[, 2]
  )

missing_country <- sum(is.na(cp_long$country))
cat("Rows where country could not be extracted: ", missing_country, "\n\n")

if (missing_country > 0) {
  cat("Examples of series_name with missing country extraction (first 10):\n")
  print(head(cp_long$series_name[is.na(cp_long$country)], 10))
  cat("\n")
}

cp_long <- cp_long |>
  dplyr::filter(!is.na(country))

cat("Distinct countries extracted: ", dplyr::n_distinct(cp_long$country), "\n\n")

# =========================================
# 7. Parse numeric values + handle missing markers
# =========================================
print_section("7. Parse numeric values (handle ':' and separators)")

cp_long <- cp_long |>
  dplyr::mutate(
    value_raw = dplyr::na_if(value_raw, ""),
    value_raw = dplyr::na_if(value_raw, ":"),
    cp_count = readr::parse_number(
      value_raw,
      locale = readr::locale(decimal_mark = ".", grouping_mark = ",")
    )
  )

cat("Share missing (NA) in cp_count: ",
    round(mean(is.na(cp_long$cp_count)), 4), "\n\n")

# =========================================
# 8. Build final country-year panel + enforce uniqueness
# =========================================
print_section("8. Build final panel + duplicate check")

cp_panel <- cp_long |>
  dplyr::group_by(country, year) |>
  dplyr::summarise(
    cp_count = dplyr::first(cp_count),
    n_rows = dplyr::n(),
    .groups = "drop"
  )

dupes <- sum(cp_panel$n_rows > 1)
cat("Country-year cells with duplicates (n_rows > 1): ", dupes, "\n\n")

if (dupes > 0) {
  cat("ERROR: Found non-unique country-year cells. Examples:\n")
  print(cp_panel |> dplyr::filter(n_rows > 1) |> head(20))
  stop("Non-unique country-year cells detected. Fix series filtering before proceeding.")
}

cp_panel <- cp_panel |>
  dplyr::select(country, year, cp_count) |>
  dplyr::arrange(country, year)

cat("Final panel summary:\n")
cat(" - countries: ", dplyr::n_distinct(cp_panel$country), "\n")
cat(" - years:     ", min(cp_panel$year, na.rm = TRUE), "–", max(cp_panel$year, na.rm = TRUE), "\n")
cat(" - rows:      ", nrow(cp_panel), "\n\n")

# =========================================
# 9. Diagnostics: coverage and missingness (save)
# =========================================
print_section("9. Diagnostics: coverage and missingness")

miss_by_country <- cp_panel |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    n_missing = sum(is.na(cp_count)),
    share_missing = mean(is.na(cp_count)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(n_missing), country)

coverage_by_year <- cp_panel |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing = sum(is.na(cp_count)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Top missingness by country (first 10):\n")
print(head(miss_by_country, 10))
cat("\n")

cat("Coverage by year (first 10):\n")
print(head(coverage_by_year, 10))
cat("\n")

readr::write_csv(miss_by_country, miss_country_csv)
readr::write_csv(coverage_by_year, coverage_year_csv)

# =========================================
# 10. Save intermediate outputs + preview
# =========================================
print_section("10. Save outputs + preview")

readr::write_csv(cp_panel, out_csv)
saveRDS(cp_panel, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (first 12 rows):\n")
print(head(cp_panel, 12))
cat("\n")

# =========================================
# 11. End + close log
# =========================================
cat("\n=========================================\n")
cat("06_ecb_card_counts_import_tidy.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()
