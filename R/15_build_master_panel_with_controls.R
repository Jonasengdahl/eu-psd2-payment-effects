# scripts/15_build_master_panel_with_controls.R
# ============================================================
# Build master panel WITH controls (GDPpc + broadband)
# This is the dataset used for controlled DiD specifications.
#
# Inputs:
#   - data/processed/panel/master_panel_payments_country_year.rds  (from script 14)
#   - data/processed/controls/gdppc_country_year.rds
#   - data/processed/controls/broadband_country_year.rds
#
# Outputs:
#   - data/processed/panel/master_panel_payments_controls_country_year.(rds|csv)
#   - outputs/tables/coverage_master_panel_payments_controls.csv
#   - outputs/logs/15_build_master_panel_with_controls.log
# ============================================================

source(here::here("scripts/00_setup.R"))

log_file <- here::here("outputs/logs/15_build_master_panel_with_controls.log")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
sink(log_file, split = TRUE)

cat("============================================================\n")
cat("15_build_master_panel_with_controls.R\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("============================================================\n\n")

read_rds_safely <- function(path) {
  if (!file.exists(path)) stop("File not found: ", path)
  readRDS(path)
}

assert_unique_key <- function(df, key_vars = c("country", "year"), df_name = "data") {
  dup_n <- df %>%
    dplyr::count(dplyr::across(dplyr::all_of(key_vars))) %>%
    dplyr::filter(n > 1) %>%
    nrow()
  if (dup_n > 0) stop("Duplicate country-year rows detected in ", df_name, ".")
  invisible(TRUE)
}

# ---- Analysis window for “control completeness” decisions
year_min <- 2012L
year_max <- 2023L

# ---- Load payments-only master (from script 14)
path_master_pay <- here::here("data/processed/panel/master_panel_payments_country_year.rds")
master <- read_rds_safely(path_master_pay)

cat("Loaded payments master:\n  ", path_master_pay, "\n", sep = "")
cat("Rows: ", nrow(master), " | countries: ", dplyr::n_distinct(master$country),
    " | years: ", min(master$year), "-", max(master$year), "\n\n", sep = "")

# ---- Load controls
path_ctrl_gdppc     <- here::here("data/processed/controls/gdppc_country_year.rds")
path_ctrl_broadband <- here::here("data/processed/controls/broadband_country_year.rds")

gdppc_ctrl     <- read_rds_safely(path_ctrl_gdppc)
broadband_ctrl <- read_rds_safely(path_ctrl_broadband)

cat("Loaded controls:\n")
cat("  gdppc: ", path_ctrl_gdppc, "\n", sep = "")
cat("  broadband: ", path_ctrl_broadband, "\n\n", sep = "")

# ---- Structure checks
stopifnot(all(c("country", "year", "gdppc_ppp_const") %in% names(gdppc_ctrl)))
stopifnot(all(c("country", "year", "broadband_per_100") %in% names(broadband_ctrl)))

assert_unique_key(gdppc_ctrl, df_name = "gdppc_ctrl")
assert_unique_key(broadband_ctrl, df_name = "broadband_ctrl")
assert_unique_key(master, df_name = "master_payments")

gdppc_ctrl <- gdppc_ctrl %>%
  dplyr::mutate(country = as.character(country), year = as.integer(year)) %>%
  dplyr::rename(gdppc = gdppc_ppp_const)

broadband_ctrl <- broadband_ctrl %>%
  dplyr::mutate(country = as.character(country), year = as.integer(year)) %>%
  dplyr::rename(broadband = broadband_per_100)

master <- master %>% dplyr::mutate(country = as.character(country), year = as.integer(year))

# ---- Merge controls (left join so the payments universe is preserved)
cat("Merging controls onto master...\n")
master_ctrl <- master %>%
  dplyr::left_join(gdppc_ctrl %>% dplyr::select(country, year, gdppc, log_gdppc), by = c("country", "year")) %>%
  dplyr::left_join(broadband_ctrl %>% dplyr::select(country, year, broadband, log1p_broadband), by = c("country", "year"))

cat("Rows after merge: ", nrow(master_ctrl), "\n\n", sep = "")

# ============================================================
# EU sample restriction (controlled specifications)
#
# The controlled DiD specifications require complete coverage
# for both macro controls:
#   - GDP per capita (PPP)
#   - Broadband penetration
#
# In the World Bank control data, three EU countries have
# missing control observations in the estimation window (2012–2023):
#   DK, LU, MT
#
# As documented in the thesis, these countries are excluded from
# the EU treated sample ONLY in specifications that include controls.
#
# The payments-only master panel (script 14) retains all EU countries.
# ============================================================

# ---- Diagnostics: control missingness in estimation window (2012–2023)
cat("Diagnostics: missing controls in estimation window ", year_min, "–", year_max, "\n", sep = "")
mw <- master_ctrl %>%
  dplyr::filter(year >= year_min, year <= year_max)

miss_ctrl_by_country <- mw %>%
  dplyr::group_by(country, is_eu, source_group) %>%
  dplyr::summarise(
    n_rows = dplyr::n(),
    miss_gdppc = mean(is.na(gdppc)),
    miss_broadband = mean(is.na(broadband)),
    any_missing_controls = any(is.na(gdppc) | is.na(broadband)),
    .groups = "drop"
  ) %>%
  dplyr::arrange(dplyr::desc(any_missing_controls), country)

print(miss_ctrl_by_country)
cat("\n")

missing_countries <- miss_ctrl_by_country %>%
  dplyr::filter(any_missing_controls) %>%
  dplyr::pull(country)

cat("Countries with ANY missing controls (", year_min, "–", year_max, "): ",
    if (length(missing_countries) == 0) "None" else paste(missing_countries, collapse = ", "),
    "\n\n", sep = "")

# ---- Optional “allowed” missingness list (your thesis rule)
allowed_missing <- c("DK", "LU", "MT")
unexpected_missing <- setdiff(missing_countries, allowed_missing)

cat("Allowed missing (per thesis): ", paste(allowed_missing, collapse = ", "), "\n", sep = "")
cat("Unexpected missing controls: ",
    if (length(unexpected_missing) == 0) "None" else paste(unexpected_missing, collapse = ", "),
    "\n\n", sep = "")

# If you want strict enforcement, uncomment:
# if (length(unexpected_missing) > 0) stop("Unexpected missing controls for: ", paste(unexpected_missing, collapse = ", "))

# ---- Save outputs
out_rds <- here::here("data/processed/panel/master_panel_payments_controls_country_year.rds")
out_csv <- here::here("data/processed/panel/master_panel_payments_controls_country_year.csv")
out_cov <- here::here("outputs/tables/coverage_master_panel_payments_controls.csv")

dir.create(dirname(out_rds), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_cov), recursive = TRUE, showWarnings = FALSE)

saveRDS(master_ctrl, out_rds)
readr::write_csv(master_ctrl, out_csv)
readr::write_csv(miss_ctrl_by_country, out_cov)

cat("Saved:\n")
cat("  ", out_rds, "\n", sep = "")
cat("  ", out_csv, "\n", sep = "")
cat("Saved control coverage table:\n")
cat("  ", out_cov, "\n\n", sep = "")

cat("15_build_master_panel_with_controls.R complete.\n")
sink()
