# scripts/11_broadband_import_tidy.R
# ============================================================
# Control variable import + tidy: Broadband penetration
#
# Source: World Bank (WDI), indicator IT.NET.BBND.P2
# Definition: Fixed broadband subscriptions (per 100 people)
#
# Outputs:
#   Intermediate (tidy, full time coverage):
#     - data/intermediate/controls/broadband/broadband_country_year_intermediate.rds
#
#   Processed (analysis-ready, trimmed to thesis window):
#     - data/processed/controls/broadband_country_year.rds
#     - data/processed/controls/broadband_country_year.csv
#
# Notes:
#   - One combined dataset for EU + US/CA to simplify merges and avoid
#     divergent cleaning across groups.
#   - Intermediate keeps all years available in the raw download.
#   - Processed restricts to 2012–2023 (your estimation window).
#   - We also create log(1+x) as a stable transformation for regressions.
# ============================================================

source(here::here("scripts/00_setup.R"))

message("============================================================")
message("11_broadband_import_tidy.R")
message("Timestamp: ", Sys.time())
message("============================================================")

# ---- 1) Paths ----------------------------------------------------------
in_raw <- here::here("data/raw/controls/worldbank/broadband_subscriptions_per_100.csv")

out_int_dir <- here::here("data/intermediate/controls/broadband")
out_int_rds <- here::here("data/intermediate/controls/broadband/broadband_country_year_intermediate.rds")

out_proc_dir <- here::here("data/processed/controls")
out_proc_rds <- here::here("data/processed/controls/broadband_country_year.rds")
out_proc_csv <- here::here("data/processed/controls/broadband_country_year.csv")

dir.create(out_int_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_proc_dir, recursive = TRUE, showWarnings = FALSE)

stopifnot(file.exists(in_raw))

# ---- 2) Analysis window (processed output) ------------------------------
year_min <- 2012L
year_max <- 2023L

# ---- 3) ISO mapping (ISO2 used in panel -> ISO3 used by World Bank) ----
iso2_to_iso3 <- tibble::tribble(
  ~country, ~iso3,
  "AT","AUT","BE","BEL","BG","BGR","CY","CYP","CZ","CZE","DE","DEU","EE","EST",
  "ES","ESP","FI","FIN","FR","FRA","GR","GRC","HR","HRV","HU","HUN","IE","IRL",
  "IT","ITA","LT","LTU","LV","LVA","NL","NLD","PL","POL","PT","PRT","RO","ROU",
  "SE","SWE","SI","SVN","SK","SVK",
  "US","USA","CA","CAN"
)

# ---- 4) Read raw World Bank file ---------------------------------------
raw <- readr::read_csv(in_raw, skip = 4, show_col_types = FALSE)
stopifnot(all(c("Country Name", "Country Code") %in% names(raw)))

# ---- 5) Reshape to country-year long -----------------------------------
df_long <- raw %>%
  dplyr::select(`Country Code`, tidyselect::matches("^[0-9]{4}$")) %>%
  tidyr::pivot_longer(
    cols = tidyselect::matches("^[0-9]{4}$"),
    names_to = "year",
    values_to = "broadband_per_100"
  ) %>%
  dplyr::mutate(
    year = as.integer(year),
    broadband_per_100 = as.numeric(broadband_per_100)
  )

# ---- 6) Harmonise to ISO2 country codes used in payments panel ----------
broadband <- iso2_to_iso3 %>%
  dplyr::left_join(df_long, by = c("iso3" = "Country Code")) %>%
  dplyr::select(country, year, broadband_per_100) %>%
  dplyr::mutate(
    log1p_broadband = dplyr::if_else(
      !is.na(broadband_per_100),
      log1p(broadband_per_100),
      NA_real_
    )
  ) %>%
  dplyr::arrange(country, year)

# ---- 7) Save intermediate + processed ----------------------------------
# Intermediate: keep full time coverage
saveRDS(broadband, out_int_rds)

# Processed: restrict to thesis analysis window
broadband_proc <- broadband %>%
  dplyr::filter(year >= year_min, year <= year_max)

saveRDS(broadband_proc, out_proc_rds)
readr::write_csv(broadband_proc, out_proc_csv)

message("Saved intermediate (all years): ", out_int_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_csv)

# ---- 8) Quick coverage checks ------------------------------------------
message("Quick coverage check (processed window, non-missing broadband):")
print(
  broadband_proc %>%
    dplyr::group_by(country) %>%
    dplyr::summarise(
      year_min = min(year[!is.na(broadband_per_100)], na.rm = TRUE),
      year_max = max(year[!is.na(broadband_per_100)], na.rm = TRUE),
      n_years  = sum(!is.na(broadband_per_100)),
      .groups = "drop"
    )
)

message("Row count (processed): ", nrow(broadband_proc))
message("Expected rows if fully complete: ", length(unique(broadband_proc$country)) * (year_max - year_min + 1L))

message("11_broadband_import_tidy.R complete.")
