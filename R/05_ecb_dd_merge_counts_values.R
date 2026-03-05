# scripts/05_ecb_dd_merge_counts_values.R
# =========================================
# Merge ECB direct debit received counts + values (EU25, annual)
# Objective:
#   - Load intermediate country-year panels for DD received counts and values
#   - Validate uniqueness + key alignment
#   - Merge into one country-year panel
#   - Save processed outputs + diagnostics + logs
#
# Inputs:
#   data/intermediate/ecb/direct_debits/dd_received_counts_country_year_intermediate.{csv|rds}
#   data/intermediate/ecb/direct_debits/dd_received_values_country_year_intermediate.{csv|rds}
#
# Outputs:
#   data/processed/ecb/direct_debits/dd_country_year.csv
#   data/processed/ecb/direct_debits/dd_country_year.rds
#   outputs/tables/dd_merge_key_coverage.csv
#   outputs/tables/dd_merge_missingness_by_country.csv
#   outputs/tables/dd_merge_country_coverage.csv
#   outputs/tables/dd_merge_summary.txt
#   outputs/logs/05_ecb_dd_merge_counts_values_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "05_ecb_dd_merge_counts_values_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("05_ecb_dd_merge_counts_values.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Paths
# =========================================
in_dir <- here::here("data/intermediate/ecb/direct_debits")

counts_rds <- file.path(in_dir, "dd_counts_country_year_intermediate.rds")
counts_csv <- file.path(in_dir, "dd_counts_country_year_intermediate.csv")

values_rds <- file.path(in_dir, "dd_received_values_country_year_intermediate.rds")
values_csv <- file.path(in_dir, "dd_received_values_country_year_intermediate.csv")

out_dir <- here::here("data/processed/ecb/direct_debits")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "dd_country_year.csv")
out_rds <- file.path(out_dir, "dd_country_year.rds")

key_cov_csv <- here::here("outputs/tables", "dd_merge_key_coverage.csv")
miss_country_csv <- here::here("outputs/tables", "dd_merge_missingness_by_country.csv")
coverage_year_csv <- here::here("outputs/tables", "dd_merge_country_coverage.csv")
summary_txt <- here::here("outputs/tables", "dd_merge_summary.txt")

cat("Inputs:\n")
cat(" - ", counts_csv, "\n")
cat(" - ", counts_rds, "\n")
cat(" - ", values_csv, "\n")
cat(" - ", values_rds, "\n\n")

cat("Outputs:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Diagnostics:\n")
cat(" - ", key_cov_csv, "\n")
cat(" - ", miss_country_csv, "\n")
cat(" - ", coverage_year_csv, "\n")
cat(" - ", summary_txt, "\n")
cat(" - ", log_file, "\n\n")

# =========================================
# 2. Load intermediate DD panels (prefer RDS)
# =========================================
print_section("2. Load intermediate DD panels")

load_panel <- function(rds_path, csv_path) {
  if (file.exists(rds_path)) {
    x <- readRDS(rds_path)
    src <- rds_path
  } else if (file.exists(csv_path)) {
    x <- readr::read_csv(
      csv_path,
      show_col_types = FALSE,
      progress = FALSE,
      col_types = readr::cols(
        country = readr::col_character(),
        year = readr::col_integer(),
        .default = readr::col_double()
      )
    )
    src <- csv_path
  } else {
    stop("Missing both RDS and CSV: ", rds_path, " / ", csv_path)
  }
  list(data = x, source = src)
}

counts_obj <- load_panel(counts_rds, counts_csv)
values_obj <- load_panel(values_rds, values_csv)

dd_counts <- counts_obj$data
dd_values <- values_obj$data

cat("dd_counts loaded from:\n  ", counts_obj$source, "\n")
cat("dd_values loaded from:\n  ", values_obj$source, "\n\n")

cat("dd_counts dims: ", nrow(dd_counts), " rows × ", ncol(dd_counts), " cols\n")
cat("dd_values dims: ", nrow(dd_values), " rows × ", ncol(dd_values), " cols\n\n")

cat("dd_counts columns: ", paste(names(dd_counts), collapse = ", "), "\n")
cat("dd_values columns: ", paste(names(dd_values), collapse = ", "), "\n\n")

stopifnot(all(c("country", "year") %in% names(dd_counts)))
stopifnot(all(c("country", "year") %in% names(dd_values)))

dd_counts <- dd_counts |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

dd_values <- dd_values |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

counts_measure <- setdiff(names(dd_counts), c("country", "year"))
values_measure <- setdiff(names(dd_values), c("country", "year"))

if (length(counts_measure) != 1) {
  stop("dd_counts should have exactly 1 measure column. Found: ",
       paste(counts_measure, collapse = ", "))
}
if (length(values_measure) != 1) {
  stop("dd_values should have exactly 1 measure column. Found: ",
       paste(values_measure, collapse = ", "))
}

counts_measure <- counts_measure[[1]]
values_measure <- values_measure[[1]]

# =========================================
# 3. Validate uniqueness + key alignment
# =========================================
print_section("3. Validate uniqueness + key alignment")

count_dupes <- dd_counts |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

value_dupes <- dd_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) rows in dd_counts: ", nrow(count_dupes), "\n")
cat("Duplicate (country,year) rows in dd_values: ", nrow(value_dupes), "\n\n")

if (nrow(count_dupes) > 0) {
  print(head(count_dupes, 20))
  stop("Stop: non-unique keys in dd_counts")
}
if (nrow(value_dupes) > 0) {
  print(head(value_dupes, 20))
  stop("Stop: non-unique keys in dd_values")
}

keys_counts <- dd_counts |> dplyr::distinct(country, year)
keys_values <- dd_values |> dplyr::distinct(country, year)

only_in_counts <- dplyr::anti_join(keys_counts, keys_values, by = c("country", "year"))
only_in_values <- dplyr::anti_join(keys_values, keys_counts, by = c("country", "year"))

cat("Keys only in counts: ", nrow(only_in_counts), "\n")
cat("Keys only in values: ", nrow(only_in_values), "\n\n")

key_cov <- dplyr::full_join(
  keys_counts |> dplyr::mutate(in_counts = TRUE),
  keys_values |> dplyr::mutate(in_values = TRUE),
  by = c("country", "year")
) |>
  dplyr::mutate(
    in_counts = dplyr::if_else(is.na(in_counts), FALSE, in_counts),
    in_values = dplyr::if_else(is.na(in_values), FALSE, in_values),
    key_status = dplyr::case_when(
      in_counts & in_values ~ "in_both",
      in_counts & !in_values ~ "only_counts",
      !in_counts & in_values ~ "only_values",
      TRUE ~ "unknown"
    )
  )

key_cov_summary <- key_cov |>
  dplyr::count(key_status, name = "n") |>
  dplyr::arrange(dplyr::desc(n))


cat("Key coverage summary:\n")
print(key_cov_summary)
cat("\n")

# =========================================
# 4. Merge counts + values
# =========================================
print_section("4. Merge counts + values")

dd_merged <- dplyr::full_join(dd_counts, dd_values, by = c("country", "year"))

# Standardize expected names (just in case)
if (!"dd_received_count" %in% names(dd_merged) && counts_measure %in% names(dd_merged)) {
  dd_merged <- dd_merged |> dplyr::rename(dd_received_count = !!counts_measure)
}
if (!"dd_received_value" %in% names(dd_merged) && values_measure %in% names(dd_merged)) {
  dd_merged <- dd_merged |> dplyr::rename(dd_received_value = !!values_measure)
}

stopifnot(all(c("dd_received_count", "dd_received_value") %in% names(dd_merged)))

dd_merged <- dd_merged |>
  dplyr::arrange(country, year)

cat("Merged dims: ", nrow(dd_merged), " rows × ", ncol(dd_merged), " cols\n")
cat("Countries in merged: ", dplyr::n_distinct(dd_merged$country), "\n")
cat("Year span in merged: ",
    min(dd_merged$year, na.rm = TRUE), "–", max(dd_merged$year, na.rm = TRUE), "\n\n")

# =========================================
# 5. Diagnostics: missingness + coverage
# =========================================
print_section("5. Merge diagnostics")

miss_by_country <- dd_merged |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    missing_count = sum(is.na(dd_received_count)),
    missing_value = sum(is.na(dd_received_value)),
    share_missing_count = mean(is.na(dd_received_count)),
    share_missing_value = mean(is.na(dd_received_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(missing_count + missing_value), country)

coverage_by_year <- dd_merged |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing_count = sum(is.na(dd_received_count)),
    n_missing_value = sum(is.na(dd_received_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Top missingness by country (first 10):\n")
print(head(miss_by_country, 10))
cat("\n")

cat("Coverage by year (all):\n")
print(coverage_by_year)
cat("\n")

neg_count <- sum(dd_merged$dd_received_count < 0, na.rm = TRUE)
neg_value <- sum(dd_merged$dd_received_value < 0, na.rm = TRUE)
cat("Negative dd_received_count entries: ", neg_count, "\n", sep = "")
cat("Negative dd_received_value entries: ", neg_value, "\n\n", sep = "")

# =========================================
# 6. Save outputs
# =========================================
print_section("6. Save outputs")

readr::write_csv(dd_merged, out_csv)
saveRDS(dd_merged, out_rds)

dir.create(here::here("outputs/tables"), recursive = TRUE, showWarnings = FALSE)
readr::write_csv(key_cov_summary, key_cov_csv)
readr::write_csv(miss_by_country, miss_country_csv)
readr::write_csv(coverage_by_year, coverage_year_csv)

summary_lines <- c(
  "ECB Direct Debits (Received, Annual, EU25) — Merge Summary",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("Counts source: ", counts_obj$source),
  paste0("Values source: ", values_obj$source),
  "",
  paste0("Merged rows: ", nrow(dd_merged),
         " | years: ", min(dd_merged$year, na.rm = TRUE), "–", max(dd_merged$year, na.rm = TRUE),
         " | countries: ", dplyr::n_distinct(dd_merged$country)),
  "",
  paste0("Keys only in counts: ", nrow(only_in_counts)),
  paste0("Keys only in values: ", nrow(only_in_values)),
  "",
  paste0("Saved processed CSV: ", out_csv),
  paste0("Saved processed RDS: ", out_rds)
)

writeLines(summary_lines, summary_txt)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Saved diagnostics:\n")
cat(" - ", key_cov_csv, "\n")
cat(" - ", miss_country_csv, "\n")
cat(" - ", coverage_year_csv, "\n")
cat(" - ", summary_txt, "\n\n")

cat("Preview (first 12 rows):\n")
print(head(dd_merged, 12))
cat("\n")

# =========================================
# 7. End + close log
# =========================================
cat("\n=========================================\n")
cat("05_ecb_dd_merge_counts_values.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()

