# scripts/26_bis_dd_merge_counts_values.R
# =========================================
# Merge BIS direct debits: counts + values (US + Canada, annual)
#
# Inputs:
#   data/intermediate/bis/direct_debits/dd_counts_country_year_intermediate.(csv|rds)
#   data/intermediate/bis/direct_debits/dd_values_country_year_intermediate.(csv|rds)
#
# Outputs:
#   data/processed/bis/direct_debits/dd_country_year.csv
#   data/processed/bis/direct_debits/dd_country_year.rds
#
# Diagnostics:
#   outputs/tables/bis_dd_merge_key_coverage.csv
#   outputs/tables/bis_dd_merge_missingness_by_country.csv
#   outputs/tables/bis_dd_merge_country_coverage.csv
#   outputs/tables/bis_dd_merge_summary.txt
#   outputs/logs/26_bis_dd_merge_counts_values_log.txt
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
log_file <- here::here("outputs/logs", "26_bis_dd_merge_counts_values_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("26_bis_dd_merge_counts_values.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
counts_csv <- here::here("data/intermediate/bis/direct_debits", "dd_counts_country_year_intermediate.csv")
counts_rds <- here::here("data/intermediate/bis/direct_debits", "dd_counts_country_year_intermediate.rds")
values_csv <- here::here("data/intermediate/bis/direct_debits", "dd_values_country_year_intermediate.csv")
values_rds <- here::here("data/intermediate/bis/direct_debits", "dd_values_country_year_intermediate.rds")

stopifnot(file.exists(counts_csv) || file.exists(counts_rds))
stopifnot(file.exists(values_csv) || file.exists(values_rds))

out_dir <- here::here("data/processed/bis/direct_debits")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_csv <- file.path(out_dir, "dd_country_year.csv")
out_rds <- file.path(out_dir, "dd_country_year.rds")

diag_dir <- here::here("outputs/tables")
dir.create(diag_dir, recursive = TRUE, showWarnings = FALSE)
diag_key_cov <- file.path(diag_dir, "bis_dd_merge_key_coverage.csv")
diag_miss_country <- file.path(diag_dir, "bis_dd_merge_missingness_by_country.csv")
diag_cov_year <- file.path(diag_dir, "bis_dd_merge_country_coverage.csv")
diag_summary <- file.path(diag_dir, "bis_dd_merge_summary.txt")

cat("Inputs:\n")
cat(" - ", counts_csv, "\n")
cat(" - ", counts_rds, "\n")
cat(" - ", values_csv, "\n")
cat(" - ", values_rds, "\n\n")

cat("Outputs:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Diagnostics:\n")
cat(" - ", diag_key_cov, "\n")
cat(" - ", diag_miss_country, "\n")
cat(" - ", diag_cov_year, "\n")
cat(" - ", diag_summary, "\n")
cat(" - ", log_file, "\n\n")

# -----------------------------
# 2) Load intermediate panels
# -----------------------------
print_section("2. Load intermediate BIS DD panels (prefer RDS)")

dd_counts <- if (file.exists(counts_rds)) {
  readRDS(counts_rds)
} else {
  readr::read_csv(counts_csv, show_col_types = FALSE)
}

dd_values <- if (file.exists(values_rds)) {
  readRDS(values_rds)
} else {
  readr::read_csv(values_csv, show_col_types = FALSE)
}

cat("dd_counts loaded from:\n  ", if (file.exists(counts_rds)) counts_rds else counts_csv, "\n", sep = "")
cat("dd_values loaded from:\n  ", if (file.exists(values_rds)) values_rds else values_csv, "\n\n", sep = "")

cat("dd_counts dims: ", nrow(dd_counts), " rows × ", ncol(dd_counts), " cols\n", sep = "")
cat("dd_values dims: ", nrow(dd_values), " rows × ", ncol(dd_values), " cols\n\n", sep = "")

cat("dd_counts columns: ", paste(names(dd_counts), collapse = ", "), "\n", sep = "")
cat("dd_values columns: ", paste(names(dd_values), collapse = ", "), "\n\n", sep = "")

# Basic column checks
req_counts <- c("country", "year", "dd_count")
req_values <- c("country", "year", "dd_value")

missing_counts <- setdiff(req_counts, names(dd_counts))
missing_values <- setdiff(req_values, names(dd_values))

if (length(missing_counts) > 0) stop("dd_counts missing columns: ", paste(missing_counts, collapse = ", "))
if (length(missing_values) > 0) stop("dd_values missing columns: ", paste(missing_values, collapse = ", "))

# Ensure types
dd_counts <- dd_counts |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

dd_values <- dd_values |>
  dplyr::mutate(country = as.character(country), year = as.integer(year))

# -----------------------------
# 3) Validate uniqueness + key alignment
# -----------------------------
print_section("3. Validate uniqueness + key alignment")

dup_counts <- dd_counts |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

dup_values <- dd_values |>
  dplyr::count(country, year, name = "n") |>
  dplyr::filter(n > 1)

cat("Duplicate (country,year) rows in dd_counts: ", nrow(dup_counts), "\n", sep = "")
cat("Duplicate (country,year) rows in dd_values: ", nrow(dup_values), "\n\n", sep = "")

if (nrow(dup_counts) > 0) {
  print(head(dup_counts, 20))
  stop("Non-unique (country,year) keys in dd_counts.")
}
if (nrow(dup_values) > 0) {
  print(head(dup_values, 20))
  stop("Non-unique (country,year) keys in dd_values.")
}

keys_counts <- dd_counts |> dplyr::distinct(country, year)
keys_values <- dd_values |> dplyr::distinct(country, year)

only_in_counts <- dplyr::anti_join(keys_counts, keys_values, by = c("country", "year"))
only_in_values <- dplyr::anti_join(keys_values, keys_counts, by = c("country", "year"))
in_both <- dplyr::inner_join(keys_counts, keys_values, by = c("country", "year"))

cat("Keys only in counts: ", nrow(only_in_counts), "\n", sep = "")
cat("Keys only in values: ", nrow(only_in_values), "\n\n", sep = "")

key_cov <- dplyr::bind_rows(
  only_in_counts |> dplyr::mutate(key_status = "only_in_counts"),
  only_in_values |> dplyr::mutate(key_status = "only_in_values"),
  in_both        |> dplyr::mutate(key_status = "in_both")
) |>
  dplyr::count(key_status, name = "n") |>
  dplyr::arrange(key_status)

cat("Key coverage summary:\n")
print(key_cov)
cat("\n")

readr::write_csv(key_cov, diag_key_cov)

# -----------------------------
# 4) Merge counts + values
# -----------------------------
print_section("4. Merge counts + values")

dd_country_year <- dd_counts |>
  dplyr::inner_join(dd_values, by = c("country", "year")) |>
  dplyr::select(country, year, dd_count, dd_value) |>
  dplyr::arrange(country, year)

cat("Merged dims: ", nrow(dd_country_year), " rows × ", ncol(dd_country_year), " cols\n", sep = "")
cat("Countries in merged: ", length(unique(dd_country_year$country)), "\n", sep = "")
cat("Year span in merged: ",
    min(dd_country_year$year, na.rm = TRUE), " – ", max(dd_country_year$year, na.rm = TRUE), "\n\n", sep = "")

# -----------------------------
# 5) Merge diagnostics
# -----------------------------
print_section("5. Merge diagnostics")

miss_by_country <- dd_country_year |>
  dplyr::group_by(country) |>
  dplyr::summarise(
    n_years = dplyr::n(),
    missing_count = sum(is.na(dd_count)),
    missing_value = sum(is.na(dd_value)),
    share_missing_count = mean(is.na(dd_count)),
    share_missing_value = mean(is.na(dd_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(missing_count + missing_value), country)

coverage_by_year <- dd_country_year |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_missing_count = sum(is.na(dd_count)),
    n_missing_value = sum(is.na(dd_value)),
    .groups = "drop"
  ) |>
  dplyr::arrange(year)

cat("Missingness by country:\n")
print(miss_by_country)
cat("\n")

cat("Coverage by year:\n")
print(coverage_by_year)
cat("\n")

neg_count <- sum(dd_country_year$dd_count < 0, na.rm = TRUE)
neg_value <- sum(dd_country_year$dd_value < 0, na.rm = TRUE)
cat("Negative dd_count entries: ", neg_count, "\n", sep = "")
cat("Negative dd_value entries: ", neg_value, "\n\n", sep = "")

readr::write_csv(miss_by_country, diag_miss_country)
readr::write_csv(coverage_by_year, diag_cov_year)

summary_lines <- c(
  "BIS Direct debits merge (counts + values)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("Counts input: ", if (file.exists(counts_rds)) counts_rds else counts_csv),
  paste0("Values input: ", if (file.exists(values_rds)) values_rds else values_csv),
  "",
  paste0("Merged rows: ", nrow(dd_country_year)),
  paste0("Countries: ", paste(sort(unique(dd_country_year$country)), collapse = ", ")),
  paste0("Year span: ", min(dd_country_year$year, na.rm = TRUE), "–", max(dd_country_year$year, na.rm = TRUE)),
  "",
  paste0("Negative dd_count: ", neg_count),
  paste0("Negative dd_value: ", neg_value)
)
writeLines(summary_lines, diag_summary)

# -----------------------------
# 6) Save outputs
# -----------------------------
print_section("6. Save outputs")

readr::write_csv(dd_country_year, out_csv)
saveRDS(dd_country_year, out_rds)

cat("Saved:\n")
cat(" - ", out_csv, "\n")
cat(" - ", out_rds, "\n\n")

cat("Preview (all rows):\n")
print(dd_country_year)
cat("\n")

# -----------------------------
# 7) End
# -----------------------------
cat("\n=========================================\n")
cat("26_bis_dd_merge_counts_values.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")
