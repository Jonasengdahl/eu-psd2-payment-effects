# scripts/25_bis_dd_values_import_tidy.R
# =========================================
# Import & tidy BIS direct debits values (US + Canada, annual)
# Objective:
#   - Read BIS "Time Series Search Export" CSVs (long format with metadata header)
#   - Keep ONLY the intended series (Direct debits, Value, Annual)
#   - Tidy into a clean country-year panel
#   - Save intermediate outputs + diagnostics + logs
#
# Inputs:
#   data/raw/bis/direct_debits/values/BIS_dd_values_US_annual_raw.csv
#   data/raw/bis/direct_debits/values/BIS_dd_values_CAN_annual_raw.csv
#
# Outputs:
#   data/intermediate/bis/direct_debits/dd_values_country_year_intermediate.csv
#   data/intermediate/bis/direct_debits/dd_values_country_year_intermediate.rds
#   outputs/tables/bis_dd_values_key_coverage.csv
#   outputs/tables/bis_dd_values_missingness_by_country.csv
#   outputs/tables/bis_dd_values_country_coverage.csv
#   outputs/tables/bis_dd_values_series_metadata.txt
#   outputs/logs/25_bis_dd_values_import_tidy_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# -----------------------------
# 0) Logging (simple + safe)
# -----------------------------
log_file <- here::here("outputs/logs", "25_bis_dd_values_import_tidy_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("25_bis_dd_values_import_tidy.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
us_file <- here::here("data/raw/bis/direct_debits/values", "BIS_dd_values_US_annual_raw.csv")
ca_file <- here::here("data/raw/bis/direct_debits/values", "BIS_dd_values_CAN_annual_raw.csv")
stopifnot(file.exists(us_file), file.exists(ca_file))

out_dir <- here::here("data/intermediate/bis/direct_debits")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "dd_values_country_year_intermediate.csv")
out_rds <- file.path(out_dir, "dd_values_country_year_intermediate.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

diag_key_cov      <- file.path(diag_dir, "bis_dd_values_key_coverage.csv")
diag_miss_country <- file.path(diag_dir, "bis_dd_values_missingness_by_country.csv")
diag_cov_year     <- file.path(diag_dir, "bis_dd_values_country_coverage.csv")
meta_txt          <- file.path(diag_dir, "bis_dd_values_series_metadata.txt")

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

# -----------------------------
# 2) Read BIS export (skip metadata header)
# -----------------------------
print_section("2. Import raw BIS files")

read_bis_export <- function(path) {
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

# -----------------------------
# 3) Validate required columns
# -----------------------------
print_section("3. Validate required columns")

required_cols <- c("REP_CTY", "TIME_PERIOD", "OBS_VALUE")

assert_has_cols <- function(df, path) {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop(
      "Missing required columns in ", path, ": ", paste(missing, collapse = ", "),
      "\nColumns present: ", paste(names(df), collapse = ", ")
    )
  }
}

assert_has_cols(us_raw, us_file)
assert_has_cols(ca_raw, ca_file)

# -----------------------------
# 4) Tidy helper (filter intended series)
# -----------------------------
print_section("4. Tidy helper (filter intended series)")

tidy_bis_dd_values <- function(df, label_for_log) {
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

  # Filters (only if columns exist)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))      # annual
  }
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^V"))   # value
  }
  # Direct debits instrument type typically starts with "C"
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$INSTRUMENT_TYPE, "^C"))
  }

  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      dd_value = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  # Diagnose duplicates before aggregation (should usually be 0 here)
  dup_before <- df |>
    dplyr::count(country, year, name = "n") |>
    dplyr::filter(n > 1)

  cat(label_for_log, ": duplicate country-year rows BEFORE aggregation: ", nrow(dup_before), "\n", sep = "")
  if (nrow(dup_before) > 0) {
    cat(label_for_log, ": example duplicate keys (first 6):\n", sep = "")
    print(head(dup_before, 6))
    cat("\n")
  }

  # Aggregate to unique country-year
  out <- df |>
    dplyr::group_by(country, year) |>
    dplyr::summarise(
      dd_value = sum(dd_value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(country, year)

  # If all dd_value were NA for a country-year, sum(., na.rm=TRUE) becomes 0; fix that:
  # Detect groups where every dd_value was NA in source rows.
  all_na_groups <- df |>
    dplyr::group_by(country, year) |>
    dplyr::summarise(all_na = all(is.na(dd_value)), .groups = "drop") |>
    dplyr::filter(all_na)

  if (nrow(all_na_groups) > 0) {
    out <- out |>
      dplyr::left_join(all_na_groups, by = c("country", "year")) |>
      dplyr::mutate(dd_value = dplyr::if_else(.data$all_na, NA_real_, .data$dd_value)) |>
      dplyr::select(-all_na)
  }

  out
}

ddv_us <- tidy_bis_dd_values(us_raw, "US")
ddv_ca <- tidy_bis_dd_values(ca_raw, "CAN")

cat("US tidy dims:  ", nrow(ddv_us), " rows × ", ncol(ddv_us), " cols\n")
cat("CAN tidy dims: ", nrow(ddv_ca), " rows × ", ncol(ddv_ca), " cols\n\n")

cat("US year span:  ", min(ddv_us$year, na.rm = TRUE), "–", max(ddv_us$year, na.rm = TRUE), "\n")
cat("CAN year span: ", min(ddv_ca$year, na.rm = TRUE), "–", max(ddv_ca$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 5) Combine + validate uniqueness
# -----------------------------
print_section("5. Combine US + CAN and validate keys")

dd_values <- dplyr::bind_rows(ddv_us, ddv_ca)

dup_after <- dd_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) cells AFTER aggregation: ", nrow(dup_after), "\n\n", sep = "")
if (nrow(dup_after) > 0) {
  cat("ERROR: duplicates detected (showing up to 20):\n")
  print(head(dup_after, 20))
  stop("Non-unique (country,year) keys in BIS DD values.")
}

cat("Combined dims: ", nrow(dd_values), " rows × ", ncol(dd_values), " cols\n")
cat("Countries: ", paste(sort(unique(dd_values$country)), collapse = ", "), "\n")
cat("Year span: ", min(dd_values$year, na.rm = TRUE), "–", max(dd_values$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 6) Diagnostics
# -----------------------------
print_section("6. Diagnostics")

key_cov <- dd_values |>
  dplyr::count(country, name = "n_country_year_rows") |>
  dplyr::arrange(country)

miss_by_country <- dd_values |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    n_missing = sum(is.na(dd_value)),
    share_missing = mean(is.na(dd_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(n_missing), country)

coverage_by_year <- dd_values |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing = sum(is.na(dd_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Missingness by country:\n")
print(miss_by_country)
cat("\n")

cat("Coverage by year:\n")
print(coverage_by_year)
cat("\n")

cat("Negative dd_value entries: ", sum(dd_values$dd_value < 0, na.rm = TRUE), "\n\n", sep = "")

readr::write_csv(key_cov, diag_key_cov)
readr::write_csv(miss_by_country, diag_miss_country)
readr::write_csv(coverage_by_year, diag_cov_year)

meta_lines <- c(
  "BIS CPMI Cashless (Direct debits) — Values (US + Canada)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", us_file),
  paste0("CAN file: ", ca_file),
  "",
  "Notes:",
  "- BIS export contains metadata header lines; script reads with skip=3.",
  "- Column names cleaned by dropping label text after ':' (e.g., 'REP_CTY:Reporting country' -> 'REP_CTY').",
  "- Filters applied when present: FREQ starts with 'A' (annual), MEASURE starts with 'V' (value), INSTRUMENT_TYPE starts with 'C' (direct debits).",
  "- dd_value parsed from OBS_VALUE via parse_number().",
  "- If duplicates exist per country-year, values are summed (after filtering) to ensure unique keys."
)
writeLines(meta_lines, meta_txt)

# -----------------------------
# 7) Save outputs
# -----------------------------
print_section("7. Save outputs")

readr::write_csv(dd_values, out_csv)
saveRDS(dd_values, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(dd_values)
cat("\n")

cat("\n=========================================\n")
cat("25_bis_dd_values_import_tidy.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
