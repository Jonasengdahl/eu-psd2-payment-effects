# scripts/29_bis_card_merge_counts_values.R
# =========================================
# Merge BIS card payments counts + values (US + Canada, annual)
# Objective:
#   - Load intermediate country-year panels for card payments counts and values
#   - Validate uniqueness + key alignment
#   - Merge into one country-year panel
#   - Save processed outputs + diagnostics + logs
#
# Inputs:
#   data/intermediate/bis/card_payments/cp_counts_country_year_intermediate.{csv|rds}
#   data/intermediate/bis/card_payments/cp_values_country_year_intermediate.{csv|rds}
#
# Outputs:
#   data/processed/bis/card_payments/cp_country_year.csv
#   data/processed/bis/card_payments/cp_country_year.rds
#   outputs/tables/bis_cp_merge_key_coverage.csv
#   outputs/tables/bis_cp_merge_missingness_by_country.csv
#   outputs/tables/bis_cp_merge_country_coverage.csv
#   outputs/tables/bis_cp_merge_summary.txt
#   outputs/logs/29_bis_card_merge_counts_values_log.txt
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
log_file <- here::here("outputs/logs", "29_bis_card_merge_counts_values_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("29_bis_card_merge_counts_values.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
in_dir <- here::here("data/intermediate/bis/card_payments")

counts_rds <- file.path(in_dir, "cp_counts_country_year_intermediate.rds")
counts_csv <- file.path(in_dir, "cp_counts_country_year_intermediate.csv")

values_rds <- file.path(in_dir, "cp_values_country_year_intermediate.rds")
values_csv <- file.path(in_dir, "cp_values_country_year_intermediate.csv")

out_dir <- here::here("data/processed/bis/card_payments")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "cp_country_year.csv")
out_rds <- file.path(out_dir, "cp_country_year.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)

key_cov_csv <- file.path(diag_dir, "bis_cp_merge_key_coverage.csv")
miss_country_csv <- file.path(diag_dir, "bis_cp_merge_missingness_by_country.csv")
coverage_year_csv <- file.path(diag_dir, "bis_cp_merge_country_coverage.csv")
summary_txt <- file.path(diag_dir, "bis_cp_merge_summary.txt")

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

# -----------------------------
# 2) Load panels (prefer RDS)
# -----------------------------
print_section("2. Load intermediate BIS card panels (prefer RDS)")

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

cat("cp_counts loaded from:\n  ", counts_obj$source, "\n")
cat("cp_values loaded from:\n  ", values_obj$source, "\n\n")

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

# -----------------------------
# 3) Validate uniqueness + key alignment
# -----------------------------
print_section("3. Validate uniqueness + key alignment")

count_dupes <- cp_counts |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

value_dupes <- cp_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) rows in cp_counts: ", nrow(count_dupes), "\n")
cat("Duplicate (country,year) rows in cp_values: ", nrow(value_dupes), "\n\n")

if (nrow(count_dupes) > 0) {
  print(head(count_dupes, 20))
  stop("Stop: non-unique keys in cp_counts")
}
if (nrow(value_dupes) > 0) {
  print(head(value_dupes, 20))
  stop("Stop: non-unique keys in cp_values")
}

keys_counts <- cp_counts |> dplyr::distinct(country, year)
keys_values <- cp_values |> dplyr::distinct(country, year)

only_in_counts <- dplyr::anti_join(keys_counts, keys_values, by = c("country", "year"))
only_in_values <- dplyr::anti_join(keys_values, keys_counts, by = c("country", "year"))

cat("Keys only in counts: ", nrow(only_in_counts), "\n")
cat("Keys only in values: ", nrow(only_in_values), "\n\n")

key_cov <- dplyr::full_join(
  dplyr::distinct(cp_counts, country, year) |> dplyr::mutate(in_counts = TRUE),
  dplyr::distinct(cp_values, country, year) |> dplyr::mutate(in_values = TRUE),
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

# -----------------------------
# 4) Merge counts + values
# -----------------------------
print_section("4. Merge counts + values")

cp_merged <- dplyr::full_join(cp_counts, cp_values, by = c("country", "year"))

# Standardize expected names (just in case)
if (!"cp_count" %in% names(cp_merged) && counts_measure %in% names(cp_merged)) {
  cp_merged <- cp_merged |> dplyr::rename(cp_count = !!counts_measure)
}
if (!"cp_value" %in% names(cp_merged) && values_measure %in% names(cp_merged)) {
  cp_merged <- cp_merged |> dplyr::rename(cp_value = !!values_measure)
}

stopifnot(all(c("cp_count", "cp_value") %in% names(cp_merged)))

cp_merged <- cp_merged |>
  dplyr::arrange(country, year)

cat("Merged dims: ", nrow(cp_merged), " rows × ", ncol(cp_merged), " cols\n")
cat("Countries in merged: ", dplyr::n_distinct(cp_merged$country), "\n")
cat("Year span in merged: ",
    min(cp_merged$year, na.rm = TRUE), "–", max(cp_merged$year, na.rm = TRUE), "\n\n")

# -----------------------------
# 5) Diagnostics: missingness + coverage
# -----------------------------
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

cat("Missingness by country:\n")
print(miss_by_country)
cat("\n")

cat("Coverage by year:\n")
print(coverage_by_year)
cat("\n")

neg_count <- sum(cp_merged$cp_count < 0, na.rm = TRUE)
neg_value <- sum(cp_merged$cp_value < 0, na.rm = TRUE)
cat("Negative cp_count entries: ", neg_count, "\n", sep = "")
cat("Negative cp_value entries: ", neg_value, "\n\n", sep = "")

# -----------------------------
# 6) Save outputs
# -----------------------------
print_section("6. Save outputs")

readr::write_csv(cp_merged, out_csv)
saveRDS(cp_merged, out_rds)

readr::write_csv(key_cov_summary, key_cov_csv)
readr::write_csv(miss_by_country, miss_country_csv)
readr::write_csv(coverage_by_year, coverage_year_csv)

summary_lines <- c(
  "BIS CPMI Cashless (Card payments, Annual) — Merge Summary (US + Canada)",
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

cat("Preview (all rows):\n")
print(cp_merged)
cat("\n")

# -----------------------------
# 7) End
# -----------------------------
cat("\n=========================================\n")
cat("29_bis_card_merge_counts_values.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
