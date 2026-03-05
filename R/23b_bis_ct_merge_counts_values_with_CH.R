# scripts/23b_bis_ct_merge_counts_values_with_CH.R
# =========================================
# Merge BIS credit transfer sent counts + values (US + Canada + Switzerland, annual)
# ROBUSTNESS PATH: includes CH
#
# Objective:
#   - Load intermediate country-year panels for CT sent counts and values
#   - Validate uniqueness + key alignment
#   - Merge into one country-year panel
#   - Save processed outputs + diagnostics + logs
#
# Inputs (ROBUSTNESS PATH: includes CH):
#   data/intermediate/bis_with_CH/credit_transfers/ct_counts_country_year_intermediate_with_CH.{csv|rds}
#   data/intermediate/bis_with_CH/credit_transfers/ct_values_country_year_intermediate_with_CH.{csv|rds}
#
# Outputs (ROBUSTNESS PATH: includes CH):
#   data/processed/bis_with_CH/credit_transfers/ct_country_year_with_CH.csv
#   data/processed/bis_with_CH/credit_transfers/ct_country_year_with_CH.rds
#
# Diagnostics (ROBUSTNESS PATH: includes CH):
#   outputs/tables/bis_ct_merge_key_coverage_with_CH.csv
#   outputs/tables/bis_ct_merge_missingness_by_country_with_CH.csv
#   outputs/tables/bis_ct_merge_country_coverage_with_CH.csv
#   outputs/tables/bis_ct_merge_summary_with_CH.txt
#   outputs/logs/23b_bis_ct_merge_counts_values_with_CH_log.txt
# =========================================

rm(list = ls())
source("scripts/00_setup.R")

# =========================================
# 0. Logging
# =========================================
log_file <- here::here("outputs/logs", "23b_bis_ct_merge_counts_values_with_CH_log.txt")
sink(log_file, split = TRUE)

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

cat("\n=========================================\n")
cat("23b_bis_ct_merge_counts_values_with_CH.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# =========================================
# 1. Paths (ROBUSTNESS PATH: includes CH)
# =========================================
in_dir <- here::here("data/intermediate/bis_with_CH/credit_transfers")

counts_rds <- file.path(in_dir, "ct_counts_country_year_intermediate_with_CH.rds")
counts_csv <- file.path(in_dir, "ct_counts_country_year_intermediate_with_CH.csv")

values_rds <- file.path(in_dir, "ct_values_country_year_intermediate_with_CH.rds")
values_csv <- file.path(in_dir, "ct_values_country_year_intermediate_with_CH.csv")

out_dir <- here::here("data/processed/bis_with_CH/credit_transfers")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "ct_country_year_with_CH.csv")
out_rds <- file.path(out_dir, "ct_country_year_with_CH.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

key_cov_csv <- file.path(diag_dir, "bis_ct_merge_key_coverage_with_CH.csv")
miss_country_csv <- file.path(diag_dir, "bis_ct_merge_missingness_by_country_with_CH.csv")
coverage_year_csv <- file.path(diag_dir, "bis_ct_merge_country_coverage_with_CH.csv")
summary_txt <- file.path(diag_dir, "bis_ct_merge_summary_with_CH.txt")

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
print_section("2. Load intermediate BIS CT panels")

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

ct_counts <- counts_obj$data
ct_values <- values_obj$data

cat("ct_counts loaded from:\n  ", counts_obj$source, "\n")
cat("ct_values loaded from:\n  ", values_obj$source, "\n\n")

cat("ct_counts dims: ", nrow(ct_counts), " rows × ", ncol(ct_counts), " cols\n")
cat("ct_values dims: ", nrow(ct_values), " rows × ", ncol(ct_values), " cols\n\n")

cat("ct_counts columns: ", paste(names(ct_counts), collapse = ", "), "\n")
cat("ct_values columns: ", paste(names(ct_values), collapse = ", "), "\n\n")

stopifnot(all(c("country", "year") %in% names(ct_counts)))
stopifnot(all(c("country", "year") %in% names(ct_values)))

ct_counts <- ct_counts |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

ct_values <- ct_values |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

# Identify measure columns robustly
counts_measure <- setdiff(names(ct_counts), c("country", "year"))
values_measure <- setdiff(names(ct_values), c("country", "year"))

if (length(counts_measure) != 1) {
  stop("ct_counts should have exactly 1 measure column. Found: ",
       paste(counts_measure, collapse = ", "))
}
if (length(values_measure) != 1) {
  stop("ct_values should have exactly 1 measure column. Found: ",
       paste(values_measure, collapse = ", "))
}

counts_measure <- counts_measure[[1]]
values_measure <- values_measure[[1]]

# Standardize expected names
if (counts_measure != "ct_sent_count") {
  ct_counts <- ct_counts |> dplyr::rename(ct_sent_count = !!counts_measure)
}
if (values_measure != "ct_sent_value") {
  ct_values <- ct_values |> dplyr::rename(ct_sent_value = !!values_measure)
}

# =========================================
# 3. Validate uniqueness + key alignment
# =========================================
print_section("3. Validate uniqueness + key alignment")

dupes_counts <- ct_counts |> dplyr::count(country, year, name = "n") |> dplyr::filter(n > 1)
dupes_values <- ct_values |> dplyr::count(country, year, name = "n") |> dplyr::filter(n > 1)

cat("Duplicate (country,year) rows in ct_counts: ", nrow(dupes_counts), "\n", sep = "")
cat("Duplicate (country,year) rows in ct_values: ", nrow(dupes_values), "\n\n", sep = "")

if (nrow(dupes_counts) > 0) {
  print(head(dupes_counts, 20))
  stop("Non-unique keys detected in ct_counts.")
}
if (nrow(dupes_values) > 0) {
  print(head(dupes_values, 20))
  stop("Non-unique keys detected in ct_values.")
}

keys_counts <- ct_counts |> dplyr::distinct(country, year)
keys_values <- ct_values |> dplyr::distinct(country, year)

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

ct_merged <- dplyr::full_join(
  ct_counts,
  ct_values,
  by = c("country", "year")
) |>
  dplyr::arrange(country, year)

cat("Merged dims: ", nrow(ct_merged), " rows × ", ncol(ct_merged), " cols\n", sep = "")
cat("Countries in merged: ", dplyr::n_distinct(ct_merged$country), "\n", sep = "")
cat("Year span in merged: ", min(ct_merged$year, na.rm = TRUE), " – ", max(ct_merged$year, na.rm = TRUE), "\n\n", sep = "")

# =========================================
# 5. Merge diagnostics
# =========================================
print_section("5. Merge diagnostics")

miss_by_country <- ct_merged |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    missing_count = sum(is.na(ct_sent_count)),
    missing_value = sum(is.na(ct_sent_value)),
    share_missing_count = mean(is.na(ct_sent_count)),
    share_missing_value = mean(is.na(ct_sent_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(missing_count + missing_value), country)

coverage_by_year <- ct_merged |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing_count = sum(is.na(ct_sent_count)),
    n_missing_value = sum(is.na(ct_sent_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Missingness by country:\n")
print(miss_by_country)
cat("\n")

cat("Coverage by year (first 15):\n")
print(head(coverage_by_year, 15))
cat("\n")

neg_count <- sum(ct_merged$ct_sent_count < 0, na.rm = TRUE)
neg_value <- sum(ct_merged$ct_sent_value < 0, na.rm = TRUE)

cat("Negative ct_sent_count entries: ", neg_count, "\n", sep = "")
cat("Negative ct_sent_value entries: ", neg_value, "\n\n", sep = "")

readr::write_csv(miss_by_country, miss_country_csv)
readr::write_csv(coverage_by_year, coverage_year_csv)

# Summary txt (useful for appendix / reproducibility notes)
summary_lines <- c(
  "BIS CPMI Cashless (Credit transfers) — Merge Summary (US + Canada + Switzerland) [with_CH]",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("Counts source: ", counts_obj$source),
  paste0("Values source: ", values_obj$source),
  "",
  paste0("Merged rows: ", nrow(ct_merged),
         " | years: ", min(ct_merged$year, na.rm = TRUE), "–", max(ct_merged$year, na.rm = TRUE),
         " | countries: ", dplyr::n_distinct(ct_merged$country)),
  "",
  paste0("Keys only in counts: ", nrow(only_in_counts)),
  paste0("Keys only in values: ", nrow(only_in_values)),
  "",
  paste0("Saved processed CSV: ", out_csv),
  paste0("Saved processed RDS: ", out_rds),
  paste0("Saved diagnostics: ", key_cov_csv, " ; ", miss_country_csv, " ; ", coverage_year_csv),
  "",
  "ROBUSTNESS PATH: includes CH."
)
writeLines(summary_lines, summary_txt)

# =========================================
# 6. Save processed outputs
# =========================================
print_section("6. Save outputs")

readr::write_csv(ct_merged, out_csv)
saveRDS(ct_merged, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Saved diagnostics:\n")
cat(" - ", key_cov_csv, "\n")
cat(" - ", miss_country_csv, "\n")
cat(" - ", coverage_year_csv, "\n")
cat(" - ", summary_txt, "\n\n")

cat("Preview (all rows):\n")
print(ct_merged)
cat("\n")

# =========================================
# 7. End + close log
# =========================================
cat("\n=========================================\n")
cat("23b_bis_ct_merge_counts_values_with_CH.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

sink()
