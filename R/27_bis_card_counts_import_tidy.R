# scripts/27_bis_card_counts_import_tidy.R
# =========================================
# Import & tidy BIS card payments counts (US + Canada, annual)
# Resolves duplicate country-year rows by aggregating across CARD_FCT
# (debit + credit function), which BIS reports separately.
#
# Inputs:
#   data/raw/bis/card_payments/counts/BIS_card_counts_US_annual_raw.csv
#   data/raw/bis/card_payments/counts/BIS_card_counts_CAN_annual_raw.csv
#
# Outputs:
#   data/intermediate/bis/card_payments/cp_counts_country_year_intermediate.csv
#   data/intermediate/bis/card_payments/cp_counts_country_year_intermediate.rds
#   outputs/tables/bis_cp_counts_series_metadata.txt
#   outputs/logs/27_bis_card_counts_import_tidy_log.txt
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
# If you ever have leftover sinks from a crash, this prevents “silent terminal” issues:
while (sink.number() > 0) sink()

log_file <- here::here("outputs/logs", "27_bis_card_counts_import_tidy_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("27_bis_card_counts_import_tidy.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
us_file <- here::here("data/raw/bis/card_payments/counts", "BIS_card_counts_US_annual_raw.csv")
ca_file <- here::here("data/raw/bis/card_payments/counts", "BIS_card_counts_CAN_annual_raw.csv")
stopifnot(file.exists(us_file), file.exists(ca_file))

out_dir <- here::here("data/intermediate/bis/card_payments")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_csv <- file.path(out_dir, "cp_counts_country_year_intermediate.csv")
out_rds <- file.path(out_dir, "cp_counts_country_year_intermediate.rds")

meta_txt <- here::here("outputs/tables", "bis_cp_counts_series_metadata.txt")
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
  # "REP_CTY:Reporting country" -> "REP_CTY"
  names(x) <- gsub(":.*$", "", names(x))
  names(x) <- trimws(names(x))
  x
}

us_raw <- read_bis_export(us_file)
ca_raw <- read_bis_export(ca_file)

cat("US raw dims:  ", nrow(us_raw), " rows × ", ncol(us_raw), " cols\n")
cat("CAN raw dims: ", nrow(ca_raw), " rows × ", ncol(ca_raw), " cols\n\n")

cat("US columns (first 12):\n")
cat(paste(head(names(us_raw), 12), collapse = ", "), "\n\n")

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
assert_has_cols(us_raw, "US file")
assert_has_cols(ca_raw, "CAN file")

# -----------------------------
# 4) Tidy helper (filter intended series + aggregate)
# -----------------------------
print_section("4. Tidy helper (filter intended series)")

tidy_bis_card_counts <- function(df, label_for_log) {

  df <- df |>
    dplyr::mutate(
      country = stringr::str_extract(.data$REP_CTY, "^[A-Z]{2}"),
      year = suppressWarnings(as.integer(stringr::str_extract(.data$TIME_PERIOD, "\\d{4}")))
    )

  # Filters (only if columns exist)
  if ("FREQ" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$FREQ, "^A"))     # annual
  }
  if ("MEASURE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$MEASURE, "^N"))  # number
  }

  # For your files, INSTRUMENT_TYPE is "F:Card and e-money payments"
  if ("INSTRUMENT_TYPE" %in% names(df)) {
    df <- df |> dplyr::filter(stringr::str_detect(.data$INSTRUMENT_TYPE, "^F"))
  }

  # Keep "all" categories where present (makes series signature stable)
  if ("DIRECTION" %in% names(df))     df <- df |> dplyr::filter(stringr::str_detect(.data$DIRECTION, "^A"))
  if ("TERMINAL_LOC" %in% names(df))  df <- df |> dplyr::filter(stringr::str_detect(.data$TERMINAL_LOC, "^A"))
  if ("DEV_STATE_TECH" %in% names(df))df <- df |> dplyr::filter(stringr::str_detect(.data$DEV_STATE_TECH, "^A"))
  if ("TERMINAL_TYPE" %in% names(df)) df <- df |> dplyr::filter(stringr::str_detect(.data$TERMINAL_TYPE, "^A"))

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

  # If all rows are NA, sum(NA, na.rm=TRUE) becomes 0 — guard against that:
  # (Not expected here, but safe.)
  out <- out |>
    dplyr::mutate(cp_count = dplyr::if_else(is.nan(cp_count), NA_real_, cp_count)) |>
    dplyr::select(country, year, cp_count)

  out
}

cp_us <- tidy_bis_card_counts(us_raw, "US")
cp_ca <- tidy_bis_card_counts(ca_raw, "CAN")

cat("US tidy dims:  ", nrow(cp_us), " rows × ", ncol(cp_us), " cols\n")
cat("CAN tidy dims: ", nrow(cp_ca), " rows × ", ncol(cp_ca), " cols\n\n")

cat("US year span:  ", min(cp_us$year, na.rm = TRUE), "–", max(cp_us$year, na.rm = TRUE), "\n")
cat("CAN year span: ", min(cp_ca$year, na.rm = TRUE), "–", max(cp_ca$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 5) Combine + validate uniqueness
# -----------------------------
print_section("5. Combine US + CAN and validate keys")

cp_counts <- dplyr::bind_rows(cp_us, cp_ca)

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
  "BIS CPMI Cashless — Card and e-money payments (Counts) — US + Canada",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("US file: ", us_file),
  paste0("CAN file: ", ca_file),
  "",
  "Series logic:",
  "- BIS export has 2 rows per year because CARD_FCT splits debit vs credit function.",
  "- Script filters to: annual (FREQ=A), number (MEASURE=N), instrument type F (card & e-money payments), plus 'All' categories where available.",
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
cat("27_bis_card_counts_import_tidy.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
