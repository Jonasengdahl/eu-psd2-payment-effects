# scripts/14b_build_master_panel_with_CH.R
# ============================================================
# Build payments-only master panel (country-year)
# ECB = EU treated group, BIS = non-EU controls
# ROBUSTNESS PATH: includes CH in BIS controls
#
# Outputs (ROBUSTNESS PATH: includes CH):
#   - data/processed/panel/master_panel_payments_country_year_with_CH.(rds|csv)
#   - outputs/tables/coverage_master_panel_payments_with_CH.csv
#   - outputs/logs/14b_build_master_panel_with_CH.log
# ============================================================

source(here::here("scripts/00_setup.R"))

log_file <- here::here("outputs/logs/14b_build_master_panel_with_CH.log")
sink(log_file, split = TRUE)

cat("============================================================\n")
cat("14b_build_master_panel_with_CH.R  (ROBUSTNESS PATH: includes CH)\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("============================================================\n\n")

# ---- Helpers ------------------------------------------------------------

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

missing_share <- function(x) mean(is.na(x))

harmonise_dd_names <- function(df, df_name = "dd") {
  # ECB style: dd_received_count/value -> dd_count/value
  if (all(c("dd_received_count", "dd_received_value") %in% names(df))) {
    df <- df %>%
      dplyr::rename(
        dd_count = dd_received_count,
        dd_value = dd_received_value
      )
  }

  # Expected standardized DD names
  required_dd_std <- c("country", "year", "dd_count", "dd_value")
  if (!all(required_dd_std %in% names(df))) {
    stop(
      df_name, " does not contain expected DD columns. Found: ",
      paste(names(df), collapse = ", ")
    )
  }
  df
}

# ---- File paths ---------------------------------------------------------

# ECB (EU member states) — unchanged
path_ecb_ct <- here::here("data/processed/ecb/credit_transfers/ct_country_year.rds")
path_ecb_cp <- here::here("data/processed/ecb/card_payments/cp_country_year.rds")
path_ecb_dd <- here::here("data/processed/ecb/direct_debits/dd_country_year.rds")

# BIS (non-EU controls) — ROBUSTNESS PATH: includes CH
path_bis_ct <- here::here("data/processed/bis_with_CH/credit_transfers/ct_country_year_with_CH.rds")
path_bis_cp <- here::here("data/processed/bis_with_CH/card_payments/cp_country_year_with_CH.rds")

# Direct debits: keep baseline BIS DD unless/until you create a with_CH DD pipeline
path_bis_dd <- here::here("data/processed/bis/direct_debits/dd_country_year.rds")

cat("Input paths (ECB):\n")
cat(" - ", path_ecb_ct, "\n")
cat(" - ", path_ecb_cp, "\n")
cat(" - ", path_ecb_dd, "\n\n")

cat("Input paths (BIS) (ROBUSTNESS PATH: includes CH where available):\n")
cat(" - ", path_bis_ct, "\n")
cat(" - ", path_bis_cp, "\n")
cat(" - ", path_bis_dd, "  (baseline DD)\n\n")

# ---- Load processed payment datasets -----------------------------------

cat("Loading processed ECB datasets...\n")
ct_ecb <- read_rds_safely(path_ecb_ct)
cp_ecb <- read_rds_safely(path_ecb_cp)
dd_ecb <- read_rds_safely(path_ecb_dd)

cat("Loading processed BIS datasets...\n")
ct_bis <- read_rds_safely(path_bis_ct)
cp_bis <- read_rds_safely(path_bis_cp)
dd_bis <- read_rds_safely(path_bis_dd)

# ---- Harmonise DD column names (ECB vs BIS) ----------------------------

dd_ecb <- harmonise_dd_names(dd_ecb, "dd_ecb")
dd_bis <- harmonise_dd_names(dd_bis, "dd_bis")

# ---- Basic checks -------------------------------------------------------

cat("\nRunning basic structure checks...\n")

required_ct <- c("country", "year", "ct_sent_count", "ct_sent_value")
required_cp <- c("country", "year", "cp_count", "cp_value")
required_dd <- c("country", "year", "dd_count", "dd_value")

stopifnot(all(required_ct %in% names(ct_ecb)))
stopifnot(all(required_cp %in% names(cp_ecb)))
stopifnot(all(required_dd %in% names(dd_ecb)))

stopifnot(all(required_ct %in% names(ct_bis)))
stopifnot(all(required_cp %in% names(cp_bis)))
stopifnot(all(required_dd %in% names(dd_bis)))

assert_unique_key(ct_ecb, df_name = "ct_ecb")
assert_unique_key(cp_ecb, df_name = "cp_ecb")
assert_unique_key(dd_ecb, df_name = "dd_ecb")

assert_unique_key(ct_bis, df_name = "ct_bis")
assert_unique_key(cp_bis, df_name = "cp_bis")
assert_unique_key(dd_bis, df_name = "dd_bis")

# Ensure types
ct_ecb <- ct_ecb %>% dplyr::mutate(year = as.integer(year), country = as.character(country))
cp_ecb <- cp_ecb %>% dplyr::mutate(year = as.integer(year), country = as.character(country))
dd_ecb <- dd_ecb %>% dplyr::mutate(year = as.integer(year), country = as.character(country))

ct_bis <- ct_bis %>% dplyr::mutate(year = as.integer(year), country = as.character(country))
cp_bis <- cp_bis %>% dplyr::mutate(year = as.integer(year), country = as.character(country))
dd_bis <- dd_bis %>% dplyr::mutate(year = as.integer(year), country = as.character(country))

# ---- Build source-specific payment panels -------------------------------

cat("\nBuilding ECB payments panel...\n")
panel_ecb <- ct_ecb %>%
  dplyr::full_join(cp_ecb, by = c("country", "year")) %>%
  dplyr::full_join(dd_ecb, by = c("country", "year")) %>%
  dplyr::mutate(
    source_group = "EU_ECB",
    is_eu = 1L
  ) %>%
  dplyr::arrange(country, year)

assert_unique_key(panel_ecb, df_name = "panel_ecb")

cat("Building BIS payments panel...\n")
panel_bis <- ct_bis %>%
  dplyr::full_join(cp_bis, by = c("country", "year")) %>%
  dplyr::full_join(dd_bis, by = c("country", "year")) %>%
  dplyr::mutate(
    source_group = "NON_EU_BIS",
    is_eu = 0L
  ) %>%
  dplyr::arrange(country, year)

assert_unique_key(panel_bis, df_name = "panel_bis")

# ---- Combine into master panel -----------------------------------------

cat("\nCombining ECB + BIS into master payments panel...\n")
master_panel_payments <- dplyr::bind_rows(panel_ecb, panel_bis) %>%
  dplyr::arrange(is_eu, country, year)

assert_unique_key(master_panel_payments, df_name = "master_panel_payments")

# ========================================================================
# ---- [EXTENSION START] Stage 3 controls merge: GDPpc + Broadband --------
# ========================================================================

cat("\nMerging controls (GDPpc + broadband) into master panel...\n")
cat("Loading processed control datasets...\n")

path_ctrl_gdppc     <- here::here("data/processed/controls/gdppc_country_year.rds")
path_ctrl_broadband <- here::here("data/processed/controls/broadband_country_year.rds")

gdppc_ctrl     <- read_rds_safely(path_ctrl_gdppc)
broadband_ctrl <- read_rds_safely(path_ctrl_broadband)

req_gdppc <- c("country", "year", "gdppc_ppp_const")
req_bb    <- c("country", "year", "broadband_per_100")

stopifnot(all(req_gdppc %in% names(gdppc_ctrl)))
stopifnot(all(req_bb %in% names(broadband_ctrl)))

assert_unique_key(gdppc_ctrl, df_name = "gdppc_ctrl")
assert_unique_key(broadband_ctrl, df_name = "broadband_ctrl")

gdppc_ctrl <- gdppc_ctrl %>%
  dplyr::mutate(year = as.integer(year), country = as.character(country))

broadband_ctrl <- broadband_ctrl %>%
  dplyr::mutate(year = as.integer(year), country = as.character(country))

gdppc_ctrl <- gdppc_ctrl %>%
  dplyr::rename(gdppc = gdppc_ppp_const)

broadband_ctrl <- broadband_ctrl %>%
  dplyr::rename(broadband = broadband_per_100)

master_panel_payments <- master_panel_payments %>%
  dplyr::left_join(
    gdppc_ctrl %>% dplyr::select(country, year, gdppc, log_gdppc),
    by = c("country", "year")
  ) %>%
  dplyr::left_join(
    broadband_ctrl %>% dplyr::select(country, year, broadband, log1p_broadband),
    by = c("country", "year")
  )

cat("Rows after merging controls: ", nrow(master_panel_payments), "\n")

# ========================================================================
# ---- [EXTENSION END] Stage 3 controls merge: GDPpc + Broadband ----------
# ========================================================================

# ---- Coverage diagnostics ----------------------------------------------

cat("\nCreating coverage diagnostics...\n")

coverage_tbl <- master_panel_payments %>%
  dplyr::group_by(country, source_group, is_eu) %>%
  dplyr::summarise(
    year_min = min(year, na.rm = TRUE),
    year_max = max(year, na.rm = TRUE),

    ct_min_year = ifelse(all(is.na(ct_sent_count)), NA_integer_, min(year[!is.na(ct_sent_count)], na.rm = TRUE)),
    ct_max_year = ifelse(all(is.na(ct_sent_count)), NA_integer_, max(year[!is.na(ct_sent_count)], na.rm = TRUE)),
    cp_min_year = ifelse(all(is.na(cp_count)), NA_integer_, min(year[!is.na(cp_count)], na.rm = TRUE)),
    cp_max_year = ifelse(all(is.na(cp_count)), NA_integer_, max(year[!is.na(cp_count)], na.rm = TRUE)),
    dd_min_year = ifelse(all(is.na(dd_count)), NA_integer_, min(year[!is.na(dd_count)], na.rm = TRUE)),
    dd_max_year = ifelse(all(is.na(dd_count)), NA_integer_, max(year[!is.na(dd_count)], na.rm = TRUE)),

    miss_ct_count = missing_share(ct_sent_count),
    miss_cp_count = missing_share(cp_count),
    miss_dd_count = missing_share(dd_count),

    miss_ct_value = missing_share(ct_sent_value),
    miss_cp_value = missing_share(cp_value),
    miss_dd_value = missing_share(dd_value),

    n_rows = dplyr::n(),
    .groups = "drop"
  ) %>%
  dplyr::arrange(source_group, country)

coverage_2012_2024 <- master_panel_payments %>%
  dplyr::filter(year >= 2012, year <= 2024) %>%
  dplyr::group_by(country, source_group, is_eu) %>%
  dplyr::summarise(
    n_rows_2012_2024 = dplyr::n(),
    complete_baseline_counts = all(!is.na(ct_sent_count) & !is.na(cp_count)),
    share_complete_baseline_counts = mean(!is.na(ct_sent_count) & !is.na(cp_count)),
    .groups = "drop"
  )

coverage_tbl <- coverage_tbl %>%
  dplyr::left_join(coverage_2012_2024, by = c("country", "source_group", "is_eu"))

# ---- Save outputs -------------------------------------------------------

out_rds <- here::here("data/processed/panel/master_panel_payments_country_year_with_CH.rds")
out_csv <- here::here("data/processed/panel/master_panel_payments_country_year_with_CH.csv")
out_cov <- here::here("outputs/tables/coverage_master_panel_payments_with_CH.csv")

cat("\nSaving master panel:\n  ", out_rds, "\n  ", out_csv, "\n")
saveRDS(master_panel_payments, out_rds)
readr::write_csv(master_panel_payments, out_csv)

cat("Saving coverage table:\n  ", out_cov, "\n")
readr::write_csv(coverage_tbl, out_cov)

# ---- Print summary ------------------------------------------------------

cat("\nSummary:\n")
cat("Master panel rows: ", nrow(master_panel_payments), "\n")
cat("Countries (total): ", dplyr::n_distinct(master_panel_payments$country), "\n")
cat("Years (min-max): ", min(master_panel_payments$year), " - ", max(master_panel_payments$year), "\n\n")

cat("By source group:\n")
print(master_panel_payments %>%
  dplyr::group_by(source_group) %>%
  dplyr::summarise(
    n_countries = dplyr::n_distinct(country),
    n_rows = dplyr::n(),
    min_year = min(year),
    max_year = max(year),
    .groups = "drop"
  ))

cat("\nDone.\n")
sink()
