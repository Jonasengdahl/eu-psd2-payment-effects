# scripts/28b_bis_card_values_import_tidy_with_CH.R
# =========================================
# Import & tidy BIS card payments VALUES (US + Canada + Switzerland, annual)
# ROBUSTNESS PATH: includes CH
#
# Robust version: resolves duplicate country-year rows by aggregating across CARD_FCT.
#
# Inputs (match raw filenames):
#   data/raw/bis/card_payments/values/BIS_card_values_US_annual_raw.csv
#   data/raw/bis/card_payments/values/BIS_card_values_CAN_annual_raw.csv
#   data/raw/bis/card_payments/values/BIS_cp_values_CH_terminal_annual.csv
#
# Outputs (ROBUSTNESS PATH: includes CH):
#   data/intermediate/bis_with_CH/card_payments/cp_values_country_year_intermediate_with_CH.csv
#   data/intermediate/bis_with_CH/card_payments/cp_values_country_year_intermediate_with_CH.rds
#   outputs/tables/bis_cp_values_series_metadata_with_CH.txt
#   outputs/logs/28b_bis_card_values_import_tidy_with_CH_log.txt
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
while (sink.number() > 0) sink()

log_file <- here::here("outputs/logs", "28b_bis_card_values_import_tidy_with_CH_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)

on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("28b_bis_card_values_import_tidy_with_CH.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths + controls (ROBUSTNESS PATH)
# -----------------------------
controls <- c("US", "CA", "CH")  # ROBUSTNESS PATH: includes CH

raw_dir <- here::here("data/raw/bis/card_payments/values")

# IMPORTANT: match your actual raw filenames exactly (CH differs)
in_files <- c(
  "US" = file.path(raw_dir, "BIS_card_values_US_annual_raw.csv"),
  "CA" = file.path(raw_dir, "BIS_card_values_CAN_annual_raw.csv"),
  "CH" = file.path(raw_dir, "BIS_cp_values_CH_terminal_annual.csv")  # ROBUSTNESS PATH: includes CH
)

cat("Controls (ROBUSTNESS PATH):  ", paste(controls, collapse = ", "), "\n\n")
cat("Expected input files:\n")
for (cc in names(in_files)) cat(" - ", cc, ": ", in_files[[cc]], "\n", sep = "")
cat("\n")

missing_files <- in_files[!file.exists(in_files)]
if (length(missing_files) > 0) {
  cat("ERROR: Missing raw BIS input files (ROBUSTNESS PATH includes CH):\n")
  for (cc in names(missing_files)) cat(" - ", cc, ": ", missing_files[[cc]], "\n", sep = "")
  cat("\n")
  stop("One or more required raw input files are missing. See log output above for expected paths.")
}

out_dir <- here::here("data/intermediate/bis_with_CH/card_payments")  # ROBUSTNESS PATH: includes CH
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "cp_values_country_year_intermediate_with_CH.csv")
out_rds <- file.path(out_dir, "cp_values_country_year_intermediate_with_CH.rds")

meta_txt <- here::here("outputs/tables", "bis_cp_values_series_metadata_with_CH.txt")
dir.create(dirname(meta_txt), recursive = TRUE, showWarnings = FALSE)

cat("Outputs:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n")
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

raw_list <- lapply(in_files, read_bis_export)

for (cc in names(raw_list)) {
  x <- raw_list[[cc]]
  cat(cc, " raw dims:  ", nrow(x), " rows × ", ncol(x), " cols\n", sep = "")
}
cat("\n")

cat("Example columns (first 12) from first file:\n")
print(head(names(raw_list[[1]]), 12))
cat("\n")

# -----------------------------
# 3) Validate required columns
# -----------------------------
print_section("3. Validate required columns")

required_cols <- c("REP_CTY", "TIME_PERIOD", "OBS_VALUE")

assert_has_cols <- function(df, label) {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop(
      "Missing required columns in ", label, ": ", paste(missing, collapse = ", "),
      "\nColumns present: ", paste(names(df), collapse = ", ")
    )
  }
}

for (cc in names(raw_list)) {
  assert_has_cols(raw_list[[cc]], paste0(cc, " file"))
}

# -----------------------------
# 4) Tidy helper (filter intended series + aggregate duplicates)
# -----------------------------
print_section("4. Tidy helper (filter intended series)")

filter_if_value_exists <- function(df, col, pattern) {
  # Only apply filter if at least one row matches; otherwise leave df unchanged
  if (col %in% names(df)) {
    has_match <- any(stringr::str_detect(df[[col]], pattern), na.rm = TRUE)
    if (has_match) {
      df <- df |> dplyr::filter(stringr::str_detect(.data[[col]], pattern))
    }
  }
  df
}

tidy_bis_card_values <- function(df, label_for_log) {

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

  # Core filters (only if columns exist)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))  # annual
  }
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^V"))  # value
  }

  # Keep "All" categories where present (but do NOT drop CH if those codes don't exist)
  df <- filter_if_value_exists(df, "DIRECTION", "^A")
  df <- filter_if_value_exists(df, "TERMINAL_LOC", "^A")
  df <- filter_if_value_exists(df, "DEV_STATE_TECH", "^A")
  df <- filter_if_value_exists(df, "TERMINAL_TYPE", "^A")

  # Parse values
  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      cp_value = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  # Diagnose duplicates BEFORE aggregation (common: CARD_FCT rows per year)
  dup <- df |>
    dplyr::count(country, year, name = "n") |>
    dplyr::filter(n > 1)

  cat(label_for_log, ": duplicate country-year rows BEFORE aggregation: ", nrow(dup), "\n", sep = "")
  if (nrow(dup) > 0) {
    cat(label_for_log, ": example duplicate keys (first 6):\n", sep = "")
    print(head(dup, 6))
    cat("\n")
  }

  # Aggregate to one row per country-year
  out <- df |>
    dplyr::group_by(country, year) |>
    dplyr::summarise(
      cp_value = sum(cp_value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(country, year)

  # If a whole country-year had only NA values, sum(na.rm=TRUE) becomes 0;
  # Convert 0 back to NA in those cases by checking original all-NA.
  all_na <- df |>
    dplyr::group_by(country, year) |>
    dplyr::summarise(all_na = all(is.na(cp_value)), .groups = "drop")

  out <- out |>
    dplyr::left_join(all_na, by = c("country", "year")) |>
    dplyr::mutate(cp_value = dplyr::if_else(all_na, as.numeric(NA), cp_value)) |>
    dplyr::select(country, year, cp_value)

  out
}

tidy_list <- mapply(
  FUN = function(df, cc) tidy_bis_card_values(df, cc),
  df = raw_list,
  cc = names(raw_list),
  SIMPLIFY = FALSE
)

for (cc in names(tidy_list)) {
  x <- tidy_list[[cc]]
  cat(cc, " tidy dims:  ", nrow(x), " rows × ", ncol(x), " cols\n", sep = "")
  if (nrow(x) > 0) {
    cat(cc, " year span:  ", min(x$year, na.rm = TRUE), "–", max(x$year, na.rm = TRUE), "\n\n", sep = "")
  } else {
    cat(cc, " year span:  (no rows after filtering)\n\n", sep = "")
  }
}

# -----------------------------
# 5) Combine controls and validate keys
# -----------------------------
print_section("5. Combine controls and validate keys")

cp_values <- dplyr::bind_rows(tidy_list) |>
  dplyr::filter(.data$country %in% controls)  # ROBUSTNESS PATH: includes CH

dup2 <- cp_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) cells AFTER aggregation: ", nrow(dup2), "\n\n", sep = "")
if (nrow(dup2) > 0) {
  cat("ERROR: duplicates detected (showing up to 20):\n")
  print(head(dup2, 20))
  stop("Non-unique (country,year) keys in BIS card values (after aggregation).")
}

cat("Combined dims:  ", nrow(cp_values), " rows × ", ncol(cp_values), " cols\n")
cat("Countries:      ", paste(sort(unique(cp_values$country)), collapse = ", "), "\n")
cat("Year span:      ", min(cp_values$year, na.rm = TRUE), "–", max(cp_values$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 6) Minimal diagnostics
# -----------------------------
print_section("6. Minimal diagnostics")

cat("Missing cp_value: ", sum(is.na(cp_values$cp_value)), " out of ", nrow(cp_values), "\n", sep = "")
cat("Negative cp_value entries: ", sum(cp_values$cp_value < 0, na.rm = TRUE), "\n\n", sep = "")

# Metadata note (appendix/repro)
meta_lines <- c(
  "BIS CPMI Cashless (Card payments) — Values (US + Canada + Switzerland) [with_CH]",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", in_files[["US"]]),
  paste0("CAN file: ", in_files[["CA"]]),
  paste0("CH file: ", in_files[["CH"]]),
  "",
  "Read/parse notes:",
  "- BIS export contains metadata header lines; script reads with skip=3.",
  "- Column names cleaned by dropping label text after ':' (e.g., 'REP_CTY:Reporting country' -> 'REP_CTY').",
  "- Filters applied when present: FREQ starts with 'A' (annual), MEASURE starts with 'V' (value).",
  "- 'All' category filters are applied only if those category codes exist in the file (ROBUSTNESS PATH: includes CH).",
  "- Duplicate country-year rows are expected due to CARD_FCT; script aggregates (sums) within country-year.",
  "- cp_value parsed from OBS_VALUE via parse_number()."
)
writeLines(meta_lines, meta_txt)

# -----------------------------
# 7) Save outputs
# -----------------------------
print_section("7. Save outputs")

readr::write_csv(cp_values, out_csv)
saveRDS(cp_values, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(cp_values)
cat("\n")

cat("\n=========================================\n")
cat("28b_bis_card_values_import_tidy_with_CH.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
