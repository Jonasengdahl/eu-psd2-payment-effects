# scripts/21_bis_ct_counts_import_tidy.R
# =========================================
# Import & tidy BIS credit transfer counts (US + Canada, annual)
# Objective:
#   - Read BIS "Time Series Search Export" CSVs (long format with metadata header)
#   - Keep ONLY the intended series (Credit transfers, Number, Annual)
#   - Tidy into a clean country-year panel
#   - Save intermediate outputs + diagnostics + logs
#
# Inputs:
#   data/raw/bis/credit_transfers/counts/BIS_ct_counts_US_annual_raw.csv
#   data/raw/bis/credit_transfers/counts/BIS_ct_counts_CAN_annual_raw.csv
#
# Outputs:
#   data/intermediate/bis/credit_transfers/ct_counts_country_year_intermediate.csv
#   data/intermediate/bis/credit_transfers/ct_counts_country_year_intermediate.rds
#   outputs/tables/bis_ct_counts_key_coverage.csv
#   outputs/tables/bis_ct_counts_missingness_by_country.csv
#   outputs/tables/bis_ct_counts_country_coverage.csv
#   outputs/tables/bis_ct_counts_series_metadata.txt
#   outputs/logs/21_bis_ct_counts_import_tidy_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "21_bis_ct_counts_import_tidy_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("21_bis_ct_counts_import_tidy.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Paths
# =========================================
us_file <- here::here("data/raw/bis/credit_transfers/counts", "BIS_ct_counts_US_annual_raw.csv")
ca_file <- here::here("data/raw/bis/credit_transfers/counts", "BIS_ct_counts_CAN_annual_raw.csv")

stopifnot(file.exists(us_file), file.exists(ca_file))

out_dir <- here::here("data/intermediate/bis/credit_transfers")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "ct_counts_country_year_intermediate.csv")
out_rds <- file.path(out_dir, "ct_counts_country_year_intermediate.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

diag_key_cov <- file.path(diag_dir, "bis_ct_counts_key_coverage.csv")
diag_miss_country <- file.path(diag_dir, "bis_ct_counts_missingness_by_country.csv")
diag_cov_year <- file.path(diag_dir, "bis_ct_counts_country_coverage.csv")
meta_txt <- file.path(diag_dir, "bis_ct_counts_series_metadata.txt")

cat("Inputs:\n")
cat(" - ", us_file, "\n")
cat(" - ", ca_file, "\n\n")

cat("Outputs:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Diagnostics:\n")
cat(" - ", diag_key_cov, "\n")
cat(" - ", diag_miss_country, "\n")
cat(" - ", diag_cov_year, "\n")
cat(" - ", meta_txt, "\n")
cat(" - ", log_file, "\n\n")

# =========================================
# 2. Read BIS export (skip metadata header)
# =========================================
print_section("2. Import raw BIS files")

read_bis_export <- function(path) {
  # BIS "Time Series Search Export" format typically has:
  # line 1: metadata
  # line 2: metadata
  # line 3: blank
  # line 4: header row (with colon labels)
  x <- readr::read_csv(
    path,
    skip = 3,
    show_col_types = FALSE,
    progress = FALSE,
    trim_ws = TRUE,
    col_types = readr::cols(.default = readr::col_character())
  )

  # Clean column names: "REP_CTY:Reporting country" -> "REP_CTY"
  names(x) <- gsub(":.*$", "", names(x))
  names(x) <- trimws(names(x))

  x
}

us_raw <- read_bis_export(us_file)
ca_raw <- read_bis_export(ca_file)

cat("US raw dims:  ", nrow(us_raw), " rows × ", ncol(us_raw), " cols\n")
cat("CAN raw dims: ", nrow(ca_raw), " rows × ", ncol(ca_raw), " cols\n\n")

cat("US columns (first 12):\n")
print(head(names(us_raw), 12))
cat("\n")

# =========================================
# 3. Tidy helper
# =========================================
print_section("3. Tidy helper (standardize columns + filter intended series)")

required_cols <- c("REP_CTY", "TIME_PERIOD", "OBS_VALUE")

assert_has_cols <- function(df, path) {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns in ", path, ": ", paste(missing, collapse = ", "),
         "\nColumns present: ", paste(names(df), collapse = ", "))
  }
}

assert_has_cols(us_raw, us_file)
assert_has_cols(ca_raw, ca_file)

tidy_bis_ct_counts <- function(df, label_for_log) {
  # Extract ISO2 from "US:United States"
  df <- df |>
    dplyr::mutate(
      country = stringr::str_extract(.data$REP_CTY, "^[A-Z]{2}"),
      year = suppressWarnings(as.integer(stringr::str_extract(.data$TIME_PERIOD, "\\d{4}")))
    )

  if (any(is.na(df$country))) {
    cat(label_for_log, ": WARNING: country extraction produced NA. Examples:\n", sep = "")
    print(head(df$REP_CTY[is.na(df$country)], 10))
    cat("\n")
  }

  if (any(is.na(df$year))) {
    cat(label_for_log, ": WARNING: year parsing produced NA. Examples:\n", sep = "")
    print(head(df$TIME_PERIOD[is.na(df$year)], 10))
    cat("\n")
  }

  # Filter to intended series (robustly, only if columns exist)
  # For BIS CPMI cashless dataset, CT instrument type is typically INSTRUMENT_TYPE == "B:Credit transfers"
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))
  }
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^N"))
  }
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$INSTRUMENT_TYPE, "^B"))
  }

  # =========================================
  # SERIES UNIQUENESS + UNIT METADATA CHECK
  # =========================================
  cat(label_for_log, ": series diagnostics\n", sep = "")

  if ("KEY" %in% names(df)) {
    cat("Distinct KEY values: ", dplyr::n_distinct(df$KEY), "\n", sep = "")
  }

  meta_cols <- intersect(
    c("FREQ", "MEASURE", "INSTRUMENT_TYPE", "DIRECTION",
      "UNIT_MEASURE", "UNIT_MULT", "UNIT_MULTIPLIER"),
    names(df)
  )

  if (length(meta_cols) > 0) {
    meta_summary <- df |>
      dplyr::select(dplyr::all_of(meta_cols)) |>
      dplyr::distinct()

    cat("Distinct metadata combinations:\n")
    print(meta_summary)
    cat("\n")
  }

  if ("OBS_VALUE" %in% names(df)) {
    cat("OBS_VALUE missing share: ",
        round(mean(df$OBS_VALUE %in% c("", ":", NA)), 4), "\n\n")
  }


  # Parse values (OBS_VALUE can arrive as numeric-like; enforce character first)
  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      ct_sent_count = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  # Keep clean output
  out <- df |>
    dplyr::select(country, year, ct_sent_count) |>
    dplyr::arrange(country, year)

  out
}

ct_us <- tidy_bis_ct_counts(us_raw, "US")
ct_ca <- tidy_bis_ct_counts(ca_raw, "CAN")

cat("US tidy dims:  ", nrow(ct_us), " rows × ", ncol(ct_us), " cols\n")
cat("CAN tidy dims: ", nrow(ct_ca), " rows × ", ncol(ct_ca), " cols\n\n")

cat("US year span:  ", min(ct_us$year, na.rm = TRUE), "–", max(ct_us$year, na.rm = TRUE), "\n")
cat("CAN year span: ", min(ct_ca$year, na.rm = TRUE), "–", max(ct_ca$year, na.rm = TRUE), "\n\n")

# =========================================
# 4. Combine + validate uniqueness
# =========================================
print_section("4. Combine US + CAN and validate keys")

ct_counts <- dplyr::bind_rows(ct_us, ct_ca)

dup <- ct_counts |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) rows: ", nrow(dup), "\n\n")
if (nrow(dup) > 0) {
  cat("ERROR: duplicates detected (showing up to 20):\n")
  print(head(dup, 20))
  stop("Non-unique (country,year) keys in BIS CT counts.")
}

cat("Combined dims: ", nrow(ct_counts), " rows × ", ncol(ct_counts), " cols\n")
cat("Countries: ", paste(sort(unique(ct_counts$country)), collapse = ", "), "\n")
cat("Year span: ", min(ct_counts$year, na.rm = TRUE), "–", max(ct_counts$year, na.rm = TRUE), "\n\n")

# =========================================
# 5. Diagnostics
# =========================================
print_section("5. Diagnostics")

# Key coverage (basic: just counts by country)
key_cov <- ct_counts |>
  dplyr::count(country, name = "n_country_year_rows") |>
  dplyr::arrange(country)

miss_by_country <- ct_counts |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    n_missing = sum(is.na(ct_sent_count)),
    share_missing = mean(is.na(ct_sent_count)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(n_missing), country)

coverage_by_year <- ct_counts |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing = sum(is.na(ct_sent_count)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Missingness by country:\n")
print(miss_by_country)
cat("\n")

cat("Coverage by year (first 15):\n")
print(head(coverage_by_year, 15))
cat("\n")

readr::write_csv(key_cov, diag_key_cov)
readr::write_csv(miss_by_country, diag_miss_country)
readr::write_csv(coverage_by_year, diag_cov_year)

# Metadata note for appendix
meta_lines <- c(
  "BIS CPMI Cashless (Credit transfers) — Counts (US + Canada)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", us_file),
  paste0("CAN file: ", ca_file),
  "",
  "Notes:",
  "- BIS export contains metadata header lines; script reads with skip=3.",
  "- Column names are cleaned by dropping label text after ':' (e.g., 'REP_CTY:Reporting country' -> 'REP_CTY').",
  "- ct_sent_count is parsed from OBS_VALUE (typically in 'Millions' per BIS 'Unit multiplier' label)."
)
writeLines(meta_lines, meta_txt)

# =========================================
# 6. Save outputs
# =========================================
print_section("6. Save outputs")

readr::write_csv(ct_counts, out_csv)
saveRDS(ct_counts, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(ct_counts)
cat("\n")

# =========================================
# 7. End + close log
# =========================================
cat("\n=========================================\n")
cat("21_bis_ct_counts_import_tidy.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()
