# scripts/27b_bis_card_counts_import_tidy_with_CH.R
# =========================================
# Import & tidy BIS card payments COUNTS (US + Canada + Switzerland, annual)
# ROBUSTNESS PATH: includes CH
#
# Robust version: resolves duplicate country-year rows by aggregating across CARD_FCT.
#
# Inputs (match raw filenames):
#   data/raw/bis/card_payments/counts/BIS_card_counts_US_annual_raw.csv
#   data/raw/bis/card_payments/counts/BIS_card_counts_CAN_annual_raw.csv
#   data/raw/bis/card_payments/counts/BIS_cp_counts_CH_terminal_annual.csv
#
# Outputs (ROBUSTNESS PATH: includes CH):
#   data/intermediate/bis_with_CH/card_payments/cp_counts_country_year_intermediate_with_CH.csv
#   data/intermediate/bis_with_CH/card_payments/cp_counts_country_year_intermediate_with_CH.rds
#   outputs/tables/bis_cp_counts_series_metadata_with_CH.txt
#   outputs/logs/27b_bis_card_counts_import_tidy_with_CH_log.txt
# =========================================

# =========================================

rm(list = ls())
source("scripts/00_setup.R")

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# -----------------------------
# 0) Logging (safe for source())
# -----------------------------
while (sink.number() > 0) sink()

log_file <- here::here("outputs/logs", "27b_bis_card_counts_import_tidy_with_CH_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("27b_bis_card_counts_import_tidy_with_CH.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths + controls (ROBUSTNESS PATH)
# -----------------------------
controls <- c("US", "CA", "CH")  # ROBUSTNESS PATH: includes CH

raw_dir <- here::here("data/raw/bis/card_payments/counts")

# Match your actual raw filenames exactly (CH differs)
in_files <- c(
  "US" = file.path(raw_dir, "BIS_card_counts_US_annual_raw.csv"),
  "CA" = file.path(raw_dir, "BIS_card_counts_CAN_annual_raw.csv"),
  "CH" = file.path(raw_dir, "BIS_cp_counts_CH_terminal_annual.csv")  # ROBUSTNESS PATH: includes CH
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
out_csv <- file.path(out_dir, "cp_counts_country_year_intermediate_with_CH.csv")
out_rds <- file.path(out_dir, "cp_counts_country_year_intermediate_with_CH.rds")

meta_txt <- here::here("outputs/tables", "bis_cp_counts_series_metadata_with_CH.txt")
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
cat(paste(head(names(raw_list[[1]]), 12), collapse = ", "), "\n\n")

# -----------------------------
# 3) Validate required columns
# -----------------------------
print_section("3. Validate required columns")

required_cols <- c("REP_CTY", "TIME_PERIOD", "OBS_VALUE")
assert_has_cols <- function(df, label) {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns in ", label, ": ", paste(missing, collapse = ", "),
         "\nColumns present: ", paste(names(df), collapse = ", "))
  }
}

for (cc in names(raw_list)) {
  assert_has_cols(raw_list[[cc]], paste0(cc, " file"))
}

# -----------------------------
# 4) Tidy helper (filter intended series + aggregate)
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

tidy_bis_card_counts <- function(df, label_for_log) {

  df <- df |>
    dplyr::mutate(
      country = stringr::str_extract(.data$REP_CTY, "^[A-Z]{2}"),
      year = suppressWarnings(as.integer(stringr::str_extract(.data$TIME_PERIOD, "\\d{4}")))
    )

  # Core identification filters (apply normally)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))     # annual
  }
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^N"))  # number
  }
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$INSTRUMENT_TYPE, "^F")) # card & e-money payments
  }

  # ROBUSTNESS PATH: CH file may not contain "All" category codes in these dimensions.
  # Apply "All" filters only if such a code exists; otherwise skip (so CH isn't dropped).
  df <- filter_if_value_exists(df, "DIRECTION", "^A")
  df <- filter_if_value_exists(df, "TERMINAL_LOC", "^A")
  df <- filter_if_value_exists(df, "DEV_STATE_TECH", "^A")
  df <- filter_if_value_exists(df, "TERMINAL_TYPE", "^A")

  # Parse numeric value
  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      cp_count_raw = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  # Diagnose duplicates BEFORE aggregation (expected because CARD_FCT splits debit/credit)
  dup_before <- df |>
    dplyr::count(country, year, name = "n") |>
    dplyr::filter(n > 1)

  if (nrow(dup_before) > 0) {
    cat(label_for_log, ": duplicate country-year rows BEFORE aggregation: ", nrow(dup_before), "\n", sep = "")
    cat(label_for_log, ": example duplicate keys (first 6):\n", sep = "")
    print(head(dup_before, 6))
    cat("\n")
  }

  # Aggregate across CARD_FCT (debit + credit) into one total per (country, year)
  out <- df |>
    dplyr::group_by(country, year) |>
    dplyr::summarise(
      cp_count = sum(cp_count_raw, na.rm = TRUE),
      n_rows_collapsed = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::arrange(country, year)

  out <- out |>
    dplyr::mutate(cp_count = dplyr::if_else(is.nan(cp_count), NA_real_, cp_count)) |>
    dplyr::select(country, year, cp_count)

  out
}

tidy_list <- mapply(
  FUN = function(df, cc) tidy_bis_card_counts(df, cc),
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
# 5) Combine + validate uniqueness
# -----------------------------
print_section("5. Combine controls and validate keys")

cp_counts <- dplyr::bind_rows(tidy_list) |>
  dplyr::filter(.data$country %in% controls)  # ROBUSTNESS PATH: includes CH

dup_after <- cp_counts |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) cells AFTER aggregation: ", nrow(dup_after), "\n\n")
if (nrow(dup_after) > 0) {
  print(head(dup_after, 20))
  stop("Still non-unique keys after aggregation. Filters need adjustment.")
}

cat("Combined dims:  ", nrow(cp_counts), " rows × ", ncol(cp_counts), " cols\n")
cat("Countries:      ", paste(sort(unique(cp_counts$country)), collapse = ", "), "\n")
cat("Year span:      ", min(cp_counts$year, na.rm = TRUE), "–", max(cp_counts$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 6) Minimal diagnostics + metadata
# -----------------------------
print_section("6. Minimal diagnostics")

cat("Missing cp_count: ", sum(is.na(cp_counts$cp_count)), " out of ", nrow(cp_counts), "\n", sep = "")
cat("Negative cp_count entries: ", sum(cp_counts$cp_count < 0, na.rm = TRUE), "\n\n", sep = "")

meta_lines <- c(
  "BIS CPMI Cashless — Card and e-money payments (Counts) — US + Canada + Switzerland [with_CH]",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", in_files[["US"]]),
  paste0("CAN file: ", in_files[["CA"]]),
  paste0("CH file: ", in_files[["CH"]]),
  "",
  "Series logic:",
  "- BIS export may have 2 rows per year because CARD_FCT splits debit vs credit function (US/CA).",
  "- Script filters to: annual (FREQ=A), number (MEASURE=N), instrument type F (card & e-money payments).",
  "- 'All' category filters are applied only if those category codes exist in the file (ROBUSTNESS PATH: includes CH).",
  "- Final cp_count is aggregated (sum) across CARD_FCT within each (country, year)."
)
writeLines(meta_lines, meta_txt)

# -----------------------------
# 7) Save outputs
# -----------------------------
print_section("7. Save outputs")

readr::write_csv(cp_counts, out_csv)
saveRDS(cp_counts, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(cp_counts)
cat("\n")

cat("\n=========================================\n")
cat("27b_bis_card_counts_import_tidy_with_CH.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
