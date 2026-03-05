# scripts/28_bis_card_values_import_tidy.R
# =========================================
# Import & tidy BIS card payments VALUES (US + Canada, annual)
# Robust version: resolves duplicate country-year rows by aggregating across CARD_FCT.
#
# Inputs:
#   data/raw/bis/card_payments/values/BIS_card_values_US_annual_raw.csv
#   data/raw/bis/card_payments/values/BIS_card_values_CAN_annual_raw.csv
#
# Outputs:
#   data/intermediate/bis/card_payments/cp_values_country_year_intermediate.csv
#   data/intermediate/bis/card_payments/cp_values_country_year_intermediate.rds
#   outputs/tables/bis_cp_values_series_metadata.txt
#   outputs/logs/28_bis_card_values_import_tidy_log.txt
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
log_file <- here::here("outputs/logs", "28_bis_card_values_import_tidy_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)

on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("28_bis_card_values_import_tidy.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
us_file <- here::here("data/raw/bis/card_payments/values", "BIS_card_values_US_annual_raw.csv")
ca_file <- here::here("data/raw/bis/card_payments/values", "BIS_card_values_CAN_annual_raw.csv")
stopifnot(file.exists(us_file), file.exists(ca_file))

out_dir <- here::here("data/intermediate/bis/card_payments")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "cp_values_country_year_intermediate.csv")
out_rds <- file.path(out_dir, "cp_values_country_year_intermediate.rds")

meta_txt <- here::here("outputs/tables", "bis_cp_values_series_metadata.txt")
dir.create(dirname(meta_txt), recursive = TRUE, showWarnings = FALSE)

cat("Inputs:\n")
cat(" - ", us_file, "\n")
cat(" - ", ca_file, "\n\n")

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
# 4) Tidy helper (filter intended series + aggregate duplicates)
# -----------------------------
print_section("4. Tidy helper (filter intended series)")

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

  # Filters (only if columns exist)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))       # annual
  }
  if ("MEASURE" %in% names(df)) {
    # Value is typically "V:Value"
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^V"))
  }

  # Instrument filter is permissive (BIS can label cards as "F:Card and e-money payments")
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(
      stringr::str_detect(stringr::str_to_lower(.data$INSTRUMENT_TYPE), "card") |
        stringr::str_detect(.data$INSTRUMENT_TYPE, "^[A-Z]")
    )
  }

  # Parse values
  df <- df |>
    dplyr::mutate(
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ""),
      OBS_VALUE = dplyr::na_if(.data$OBS_VALUE, ":"),
      cp_value = readr::parse_number(as.character(.data$OBS_VALUE))
    )

  # Diagnose duplicates BEFORE aggregation (common: two CARD_FCT rows per year)
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

cp_us <- tidy_bis_card_values(us_raw, "US")
cp_ca <- tidy_bis_card_values(ca_raw, "CAN")

cat("US tidy dims:  ", nrow(cp_us), " rows × ", ncol(cp_us), " cols\n")
cat("CAN tidy dims: ", nrow(cp_ca), " rows × ", ncol(cp_ca), " cols\n\n")

cat("US year span:  ", min(cp_us$year, na.rm = TRUE), "–", max(cp_us$year, na.rm = TRUE), "\n")
cat("CAN year span: ", min(cp_ca$year, na.rm = TRUE), "–", max(cp_ca$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 5) Combine US + CAN and validate keys
# -----------------------------
print_section("5. Combine US + CAN and validate keys")

cp_values <- dplyr::bind_rows(cp_us, cp_ca)

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
  "BIS CPMI Cashless (Card payments) — Values (US + Canada)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", us_file),
  paste0("CAN file: ", ca_file),
  "",
  "Read/parse notes:",
  "- BIS export contains metadata header lines; script reads with skip=3.",
  "- Column names cleaned by dropping label text after ':' (e.g., 'REP_CTY:Reporting country' -> 'REP_CTY').",
  "- Filters applied when present: FREQ starts with 'A' (annual), MEASURE starts with 'V' (value).",
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
cat("28_bis_card_values_import_tidy.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
