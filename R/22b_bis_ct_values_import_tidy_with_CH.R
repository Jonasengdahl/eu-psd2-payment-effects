# scripts/22b_bis_ct_values_import_tidy_with_CH.R
# =========================================
# Import & tidy BIS credit transfer values (US + Canada + Switzerland, annual)
# ROBUSTNESS PATH: includes CH
#
# Objective:
#   - Read BIS "Time Series Search Export" CSVs (long format with metadata header)
#   - Keep ONLY the intended series (Credit transfers, Value, Annual)
#   - Tidy into a clean country-year panel
#   - Save intermediate outputs + diagnostics + logs
#
# Inputs:
#   data/raw/bis/credit_transfers/values/BIS_ct_values_US_annual_raw.csv
#   data/raw/bis/credit_transfers/values/BIS_ct_values_CAN_annual_raw.csv
#   data/raw/bis/credit_transfers/values/BIS_ct_values_CH_annual_raw.csv
#
# Outputs (ROBUSTNESS PATH: includes CH):
#   data/intermediate/bis_with_CH/credit_transfers/ct_values_country_year_intermediate_with_CH.csv
#   data/intermediate/bis_with_CH/credit_transfers/ct_values_country_year_intermediate_with_CH.rds
#   outputs/tables/bis_ct_values_key_coverage_with_CH.csv
#   outputs/tables/bis_ct_values_missingness_by_country_with_CH.csv
#   outputs/tables/bis_ct_values_country_coverage_with_CH.csv
#   outputs/tables/bis_ct_values_series_metadata_with_CH.txt
#   outputs/logs/22b_bis_ct_values_import_tidy_with_CH_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "22b_bis_ct_values_import_tidy_with_CH_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("22b_bis_ct_values_import_tidy_with_CH.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Paths + controls (ROBUSTNESS PATH)
# =========================================
controls <- c("US", "CA", "CH")  # ROBUSTNESS PATH: includes CH

# Raw BIS filenames use CAN for Canada; keep internal country codes as ISO2 (CA)
file_stub <- c("US" = "US", "CA" = "CAN", "CH" = "CH")  # ROBUSTNESS PATH: includes CH

raw_dir <- here::here("data/raw/bis/credit_transfers/values")

in_files <- stats::setNames(
  file.path(raw_dir, paste0("BIS_ct_values_", unname(file_stub[controls]), "_annual_raw.csv")),
  controls
)

stopifnot(all(file.exists(in_files)))

out_dir <- here::here("data/intermediate/bis_with_CH/credit_transfers")  # ROBUSTNESS PATH: includes CH
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "ct_values_country_year_intermediate_with_CH.csv")
out_rds <- file.path(out_dir, "ct_values_country_year_intermediate_with_CH.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

diag_key_cov <- file.path(diag_dir, "bis_ct_values_key_coverage_with_CH.csv")
diag_miss_country <- file.path(diag_dir, "bis_ct_values_missingness_by_country_with_CH.csv")
diag_cov_year <- file.path(diag_dir, "bis_ct_values_country_coverage_with_CH.csv")
meta_txt <- file.path(diag_dir, "bis_ct_values_series_metadata_with_CH.txt")

cat("Controls (ROBUSTNESS PATH): ", paste(controls, collapse = ", "), "\n\n")

cat("Inputs:\n")
for (cc in names(in_files)) cat(" - ", cc, ": ", in_files[[cc]], "\n", sep = "")
cat("\n")

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
  # BIS "Time Series Search Export" format typically has 3 header/metadata lines.
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

raw_list <- lapply(in_files, read_bis_export)

for (cc in names(raw_list)) {
  x <- raw_list[[cc]]
  cat(cc, " raw dims: ", nrow(x), " rows × ", ncol(x), " cols\n", sep = "")
}
cat("\n")

cat("Example columns (first 12) from first file:\n")
print(head(names(raw_list[[1]]), 12))
cat("\n")

# =========================================
# 3. Tidy helper (standardize columns + filter intended series)
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

for (cc in names(raw_list)) {
  assert_has_cols(raw_list[[cc]], in_files[[cc]])
}

tidy_bis_ct_values <- function(df, label_for_log) {
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

  # Filter to intended series (only if columns exist)
  # FREQ: Annual (A)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))
  }
  # MEASURE: Value (V)
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^V"))
  }
  # INSTRUMENT_TYPE: Credit transfers (B)
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$INSTRUMENT_TYPE, "^B"))
  }

  # Parse values
  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      ct_sent_value = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  df |>
    dplyr::select(country, year, ct_sent_value) |>
    dplyr::arrange(country, year)
}

tidy_list <- mapply(
  FUN = function(df, cc) tidy_bis_ct_values(df, cc),
  df = raw_list,
  cc = names(raw_list),
  SIMPLIFY = FALSE
)

for (cc in names(tidy_list)) {
  x <- tidy_list[[cc]]
  cat(cc, " tidy dims: ", nrow(x), " rows × ", ncol(x), " cols\n", sep = "")
  cat(cc, " year span: ", min(x$year, na.rm = TRUE), "–", max(x$year, na.rm = TRUE), "\n\n", sep = "")
}

# =========================================
# 4. Combine + validate uniqueness
# =========================================
print_section("4. Combine controls and validate keys")

ct_values <- dplyr::bind_rows(tidy_list) |>
  dplyr::filter(.data$country %in% controls)  # ROBUSTNESS PATH: includes CH

dup <- ct_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) rows: ", nrow(dup), "\n\n")
if (nrow(dup) > 0) {
  cat("ERROR: duplicates detected (showing up to 20):\n")
  print(head(dup, 20))
  stop("Non-unique (country,year) keys in BIS CT values (with_CH).")
}

cat("Combined dims: ", nrow(ct_values), " rows × ", ncol(ct_values), " cols\n")
cat("Countries: ", paste(sort(unique(ct_values$country)), collapse = ", "), "\n")
cat("Year span: ", min(ct_values$year, na.rm = TRUE), "–", max(ct_values$year, na.rm = TRUE), "\n\n")

# =========================================
# 5. Diagnostics
# =========================================
print_section("5. Diagnostics")

key_cov <- ct_values |>
  dplyr::count(country, name = "n_country_year_rows") |>
  dplyr::arrange(country)

miss_by_country <- ct_values |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    n_missing = sum(is.na(ct_sent_value)),
    share_missing = mean(is.na(ct_sent_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(n_missing), country)

coverage_by_year <- ct_values |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing = sum(is.na(ct_sent_value)),
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

meta_lines <- c(
  "BIS CPMI Cashless (Credit transfers) — Values (US + Canada + Switzerland) [with_CH]",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", in_files[["US"]]),
  paste0("CAN file: ", in_files[["CA"]]),
  paste0("CH file: ", in_files[["CH"]]),
  "",
  "Filters applied (when columns exist):",
  "- FREQ starts with 'A' (Annual)",
  "- MEASURE starts with 'V' (Value)",
  "- INSTRUMENT_TYPE starts with 'B' (Credit transfers)",
  "",
  "Units:",
  "- Values are in local currency (Unit column in BIS export).",
  "- Unit multiplier is typically 'Millions' in these exports; ct_sent_value is stored as reported (not rescaled).",
  "- ROBUSTNESS PATH: includes CH."
)
writeLines(meta_lines, meta_txt)

# =========================================
# 6. Save outputs
# =========================================
print_section("6. Save outputs")

readr::write_csv(ct_values, out_csv)
saveRDS(ct_values, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(ct_values)
cat("\n")

# =========================================
# 7. End + close log
# =========================================
cat("\n=========================================\n")
cat("22b_bis_ct_values_import_tidy_with_CH.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()
