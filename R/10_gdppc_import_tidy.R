# scripts/10_gdppc_import_tidy.R
# ============================================================
# Control variable import + tidy: GDP per capita (PPP, constant)
#
# Source: World Bank (WDI), indicator NY.GDP.PCAP.PP.KD
#
# Outputs:
#   Intermediate (tidy, full time coverage):
#     - data/intermediate/controls/gdppc/gdppc_country_year_intermediate.rds
#
#   Processed (analysis-ready, trimmed to thesis window):
#     - data/processed/controls/gdppc_country_year.rds
#     - data/processed/controls/gdppc_country_year.csv
#
# Notes:
#   - We keep one combined dataset for EU + US/CA to simplify merges and avoid
#     divergent cleaning across groups.
#   - Intermediate keeps all years available in the raw download (transparency).
#   - Processed restricts to 2012–2023 (your estimation window).
# ============================================================

source(here::here("scripts/00_setup.R"))

message("============================================================")
message("10_gdppc_import_tidy.R")
message("Timestamp: ", Sys.time())
message("============================================================")

# ---- 1) Paths ----------------------------------------------------------
in_raw <- here::here("data/raw/controls/worldbank/gdppc_ppp_constant_2017_usd.csv")

out_int_dir <- here::here("data/intermediate/controls/gdppc")
out_int_rds <- here::here("data/intermediate/controls/gdppc/gdppc_country_year_intermediate.rds")

out_proc_dir <- here::here("data/processed/controls")
out_proc_rds <- here::here("data/processed/controls/gdppc_country_year.rds")
out_proc_csv <- here::here("data/processed/controls/gdppc_country_year.csv")

dir.create(out_int_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_proc_dir, recursive = TRUE, showWarnings = FALSE)

stopifnot(file.exists(in_raw))

# ---- 2) Analysis window (processed output) ------------------------------
year_min <- 2012L
year_max <- 2023L

# ---- 3) ISO mapping (ISO2 used in panel -> ISO3 used by World Bank) ----
# Keep explicit mapping to avoid extra dependencies.
iso2_to_iso3 <- tibble::tribble(
  ~country, ~iso3,
  "AT","AUT","BE","BEL","BG","BGR","CY","CYP","CZ","CZE","DE","DEU","EE","EST",
  "ES","ESP","FI","FIN","FR","FRA","GR","GRC","HR","HRV","HU","HUN","IE","IRL",
  "IT","ITA","LT","LTU","LV","LVA","NL","NLD","PL","POL","PT","PRT","RO","ROU",
  "SE","SWE","SI","SVN","SK","SVK",
  "US","USA","CA","CAN"
)

# ---- 4) Read raw World Bank file ---------------------------------------
# WDI "API_*.csv" files typically have 4 metadata lines before the header row.
raw <- readr::read_csv(in_raw, skip = 4, show_col_types = FALSE)

stopifnot(all(c("Country Name", "Country Code") %in% names(raw)))

# ---- 5) Reshape to country-year long -----------------------------------
df_long <- raw %>%
  dplyr::select(`Country Code`, tidyselect::matches("^[0-9]{4}$")) %>%
  tidyr::pivot_longer(
    cols = tidyselect::matches("^[0-9]{4}$"),
    names_to = "year",
    values_to = "gdppc_ppp_const"
  ) %>%
  dplyr::mutate(
    year = as.integer(year),
    gdppc_ppp_const = as.numeric(gdppc_ppp_const)
  )

# ---- 6) Harmonise to ISO2 country codes used in payments panel ----------
gdppc <- iso2_to_iso3 %>%
  dplyr::left_join(df_long, by = c("iso3" = "Country Code")) %>%
  dplyr::select(country, year, gdppc_ppp_const) %>%
  dplyr::mutate(
    log_gdppc = dplyr::if_else(gdppc_ppp_const > 0, log(gdppc_ppp_const), NA_real_)
  ) %>%
  dplyr::arrange(country, year)

# ---- 7) Save intermediate + processed ----------------------------------
# Intermediate: keep full time coverage (useful for transparency/future extensions)
saveRDS(gdppc, out_int_rds)

# Processed: restrict to thesis analysis window
gdppc_proc <- gdppc %>%
  dplyr::filter(year >= year_min, year <= year_max)

saveRDS(gdppc_proc, out_proc_rds)
readr::write_csv(gdppc_proc, out_proc_csv)

message("Saved intermediate (all years): ", out_int_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_csv)

# ---- 8) Quick coverage checks ------------------------------------------
message("Quick coverage check (processed window, non-missing gdppc):")
print(
  gdppc_proc %>%
    dplyr::group_by(country) %>%
    dplyr::summarise(
      year_min = min(year[!is.na(gdppc_ppp_const)], na.rm = TRUE),
      year_max = max(year[!is.na(gdppc_ppp_const)], na.rm = TRUE),
      n_years  = sum(!is.na(gdppc_ppp_const)),
      .groups = "drop"
    )
)

message("Row count (processed): ", nrow(gdppc_proc))
message("Expected rows if fully complete: ", length(unique(gdppc_proc$country)) * (year_max - year_min + 1L))

message("10_gdppc_import_tidy.R complete.")
