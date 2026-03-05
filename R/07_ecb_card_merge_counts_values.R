# scripts/07_ecb_card_merge_counts_values.R
# =========================================
# Merge ECB card payments counts + values (EU27, annual)
# Objective:
#   - Load intermediate country-year panels for card payments counts and values
#   - Validate uniqueness + key alignment
#   - Merge into one country-year panel
#   - Save processed outputs + diagnostics + logs
#
# Inputs:
#   data/intermediate/ecb/card_payments/cp_counts_country_year_intermediate.{csv|rds}
#   data/intermediate/ecb/card_payments/cp_values_country_year_intermediate.{csv|rds}
#
# Outputs:
#   data/processed/ecb/card_payments/cp_country_year.csv
#   data/processed/ecb/card_payments/cp_country_year.rds
#
# Diagnostics:
#   outputs/tables/cp_merge_key_coverage.csv
#   outputs/tables/cp_merge_missingness_by_country.csv
#   outputs/tables/cp_merge_country_coverage.csv
#   outputs/tables/cp_merge_summary.txt
#   outputs/logs/07_ecb_card_merge_counts_values_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "07_ecb_card_merge_counts_values_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("07_ecb_card_merge_counts_values.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Paths
# =========================================
in_dir <- here::here("data/intermediate/ecb/card_payments")

counts_rds <- file.path(in_dir, "cp_counts_country_year_intermediate.rds")
counts_csv <- file.path(in_dir, "cp_counts_country_year_intermediate.csv")

values_rds <- file.path(in_dir, "cp_values_country_year_intermediate.rds")
values_csv <- file.path(in_dir, "cp_values_country_year_intermediate.csv")

out_dir <- here::here("data/processed/ecb/card_payments")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "cp_country_year.csv")
out_rds <- file.path(out_dir, "cp_country_year.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

key_cov_csv <- file.path(diag_dir, "cp_merge_key_coverage.csv")
miss_country_csv <- file.path(diag_dir, "cp_merge_missingness_by_country.csv")
coverage_year_csv <- file.path(diag_dir, "cp_merge_country_coverage.csv")
summary_txt <- file.path(diag_dir, "cp_merge_summary.txt")

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
# 2. Load intermediate panels (prefer RDS)
# =========================================
print_section("2. Load intermediate card payments panels")

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

cp_counts <- counts_obj$data
cp_values <- values_obj$data

cat("cp_counts dims: ", nrow(cp_counts), " rows × ", ncol(cp_counts), " cols\n")
cat("cp_values dims: ", nrow(cp_values), " rows × ", ncol(cp_values), " cols\n\n")

cat("cp_counts columns: ", paste(names(cp_counts), collapse = ", "), "\n")
cat("cp_values columns: ", paste(names(cp_values), collapse = ", "), "\n\n")

stopifnot(all(c("country", "year") %in% names(cp_counts)))
stopifnot(all(c("country", "year") %in% names(cp_values)))

cp_counts <- cp_counts |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

cp_values <- cp_values |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

# Identify measure columns robustly (in case name differs slightly)
counts_measure <- setdiff(names(cp_counts), c("country", "year"))
values_measure <- setdiff(names(cp_values), c("country", "year"))

if (length(counts_measure) != 1) {
  stop("cp_counts should have exactly 1 measure column. Found: ",
       paste(counts_measure, collapse = ", "))
}
if (length(values_measure) != 1) {
  stop("cp_values should have exactly 1 measure column. Found: ",
       paste(values_measure, collapse = ", "))
}

counts_measure <- counts_measure[[1]]
values_measure <- values_measure[[1]]

# Standardize names
if (counts_measure != "cp_count") {
  cp_counts <- cp_counts |> dplyr::rename(cp_count = !!counts_measure)
}
if (values_measure != "cp_value") {
  cp_values <- cp_values |> dplyr::rename(cp_value = !!values_measure)
}

# =========================================
# 3. Validate uniqueness + key alignment
# =========================================
print_section("3. Validate uniqueness + key alignment")

dupes_counts <- cp_counts |> dplyr::count(country, year, name = "n") |> dplyr::filter(n > 1)
dupes_values <- cp_values |> dplyr::count(country, year, name = "n") |> dplyr::filter(n > 1)

cat("Duplicate (country,year) rows in cp_counts: ", nrow(dupes_counts), "\n", sep = "")
cat("Duplicate (country,year) rows in cp_values: ", nrow(dupes_values), "\n\n", sep = "")

if (nrow(dupes_counts) > 0) {
  print(head(dupes_counts, 20))
  stop("Non-unique keys detected in cp_counts.")
}
if (nrow(dupes_values) > 0) {
  print(head(dupes_values, 20))
  stop("Non-unique keys detected in cp_values.")
}

keys_counts <- cp_counts |> dplyr::distinct(country, year)
keys_values <- cp_values |> dplyr::distinct(country, year)

only_in_counts <- dplyr::anti_join(keys_counts, keys_values, by = c("country", "year"))
only_in_values <- dplyr::anti_join(keys_values, keys_counts, by = c("country", "year"))

cat("Keys only in counts: ", nrow(only_in_counts), "\n", sep = "")
cat("Keys only in values: ", nrow(only_in_values), "\n\n", sep = "")

key_coverage <- dplyr::full_join(
  keys_counts |> dplyr::mutate(in_counts = TRUE),
  keys_values |> dplyr::mutate(in_values = TRUE),
  by = c("country", "year")
) |>
  dplyr::mutate(
    in_counts = dplyr::if_else(is.na(in_counts), FALSE, in_counts),
    in_values = dplyr::if_else(is.na(in_values), FALSE, in_values),
    key_status = dplyr::case_when(
      in_counts & in_values ~ "in_both",
      in_counts & !in_values ~ "only_in_counts",
      !in_counts & in_values ~ "only_in_values",
      TRUE ~ "unknown"
    )
  )

key_summary <- key_coverage |>
  dplyr::count(key_status, name = "n") |>
  dplyr::arrange(dplyr::desc(n))

cat("Key coverage summary:\n")
print(key_summary)
cat("\n")

readr::write_csv(key_summary, key_cov_csv)

# =========================================
# 4. Merge
# =========================================
print_section("4. Merge counts + values")

cp_merged <- dplyr::full_join(
  cp_counts,
  cp_values,
  by = c("country", "year")
)

cat("Merged dims: ", nrow(cp_merged), " rows × ", ncol(cp_merged), " cols\n", sep = "")
cat("Countries in merged: ", dplyr::n_distinct(cp_merged$country), "\n", sep = "")
cat("Year span in merged: ", min(cp_merged$year, na.rm = TRUE), " – ", max(cp_merged$year, na.rm = TRUE), "\n\n", sep = "")

# =========================================
# 5. Merge diagnostics
# =========================================
print_section("5. Merge diagnostics")

miss_by_country <- cp_merged |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    missing_count = sum(is.na(cp_count)),
    missing_value = sum(is.na(cp_value)),
    share_missing_count = mean(is.na(cp_count)),
    share_missing_value = mean(is.na(cp_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(missing_count + missing_value), country)

coverage_by_year <- cp_merged |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing_count = sum(is.na(cp_count)),
    n_missing_value = sum(is.na(cp_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Top missingness by country (first 10):\n")
print(head(miss_by_country, 10))
cat("\n")

cat("Coverage by year (first 10):\n")
print(head(coverage_by_year, 10))
cat("\n")

neg_count <- sum(cp_merged$cp_count < 0, na.rm = TRUE)
neg_value <- sum(cp_merged$cp_value < 0, na.rm = TRUE)

cat("Negative cp_count entries: ", neg_count, "\n", sep = "")
cat("Negative cp_value entries: ", neg_value, "\n\n", sep = "")

readr::write_csv(miss_by_country, miss_country_csv)
readr::write_csv(coverage_by_year, coverage_year_csv)

# Short summary txt (useful for appendix/repro notes)
summary_lines <- c(
  "ECB Card Payments (Annual, EU27) — Merge Summary",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("Counts source: ", counts_obj$source),
  paste0("Values source: ", values_obj$source),
  "",
  paste0("Merged rows: ", nrow(cp_merged),
         " | years: ", min(cp_merged$year, na.rm = TRUE), "–", max(cp_merged$year, na.rm = TRUE),
         " | countries: ", dplyr::n_distinct(cp_merged$country)),
  "",
  paste0("Keys only in counts: ", nrow(only_in_counts)),
  paste0("Keys only in values: ", nrow(only_in_values)),
  "",
  paste0("Saved processed CSV: ", out_csv),
  paste0("Saved processed RDS: ", out_rds),
  paste0("Saved diagnostics: ", key_cov_csv, " ; ", miss_country_csv, " ; ", coverage_year_csv)
)
writeLines(summary_lines, summary_txt)

# =========================================
# 6. Save processed outputs
# =========================================
print_section("6. Save outputs")

readr::write_csv(cp_merged, out_csv)
saveRDS(cp_merged, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Saved diagnostics:\n")
cat(" - ", key_cov_csv, "\n")
cat(" - ", miss_country_csv, "\n")
cat(" - ", coverage_year_csv, "\n")
cat(" - ", summary_txt, "\n\n")

cat("Preview (first 12 rows):\n")
print(head(cp_merged, 12))
cat("\n")

# =========================================
# 7. End + close log
# =========================================
cat("\n=========================================\n")
cat("07_ecb_card_merge_counts_values.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()

