# scripts/12_ecomm_import_tidy.R
# ============================================================
# Control variable import + tidy: E-commerce adoption
#
# Conservative strategy (by design):
#   - EU: Eurostat isoc_ec_ibuy (keep <= 2019 only; pre-redesign)
#   - US/CA: OECD series (non-EU controls)
#   - Do NOT stitch Eurostat ibuy + ib20 into one continuous level series
#     (survey redesign in 2020 can create mechanical breaks).
#
# Outputs:
#   Intermediate:
#     - data/intermediate/controls/ecommerce/ecommerce_country_year_intermediate.rds
#   Processed (2012–2023):
#     - data/processed/controls/ecommerce_country_year.rds
#     - data/processed/controls/ecommerce_country_year.csv
# ============================================================

source(here::here("scripts/00_setup.R"))

message("============================================================")
message("12_ecomm_import_tidy.R")
message("Timestamp: ", Sys.time())
message("============================================================")

# ---- 1) Paths ----------------------------------------------------------
in_euro_ibuy <- here::here("data/raw/controls/eurostat/ecommerce_individuals_ibuy.tsv")
in_oecd      <- here::here("data/raw/controls/oecd/ecommerce_individuals_oecd.csv")

out_int_dir <- here::here("data/intermediate/controls/ecommerce")
out_int_rds <- here::here("data/intermediate/controls/ecommerce/ecommerce_country_year_intermediate.rds")

out_proc_dir <- here::here("data/processed/controls")
out_proc_rds <- here::here("data/processed/controls/ecommerce_country_year.rds")
out_proc_csv <- here::here("data/processed/controls/ecommerce_country_year.csv")

dir.create(out_int_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_proc_dir, recursive = TRUE, showWarnings = FALSE)

stopifnot(file.exists(in_euro_ibuy), file.exists(in_oecd))

# ---- 2) Window + locked country lists ----------------------------------
year_min <- 2012L
year_max <- 2023L

# Same locked EU list as baseline DiD / coverage restriction
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)
controls <- c("US","CA")

# ---- 3) Eurostat (EU): read ibuy TSV, extract one concept consistently --
message("Reading Eurostat ibuy TSV (fast reader; keeping only 2012–2019 columns)...")

# 3.1 Read header only -----------------------------------------------------
hdr <- readr::read_delim(
  file = in_euro_ibuy,
  delim = "\t",
  n_max = 0,
  show_col_types = FALSE,
  progress = FALSE
)

hdr_names_raw  <- names(hdr)
hdr_names_trim <- trimws(hdr_names_raw)

key_col_raw  <- hdr_names_raw[1]
key_col_trim <- trimws(key_col_raw)

keep_years <- as.character(2012:2019)

# Keep original raw names for col_select matching, but match years on trimmed names
keep_cols <- hdr_names_raw[hdr_names_trim %in% c(key_col_trim, keep_years)]

if (length(keep_cols) <= 1) {
  stop(
    "Could not find year columns 2012–2019 in Eurostat header.\n",
    "Observed header names include: ",
    paste(utils::head(hdr_names_raw, 25), collapse = ", "),
    "\nNote: Eurostat often uses trailing spaces like '2012 '."
  )
}

# 3.2 Selective read (key + 2012–2019 only) --------------------------------
euro_raw <- tryCatch(
  {
    readr::read_delim(
      file = in_euro_ibuy,
      delim = "\t",
      col_select = dplyr::all_of(keep_cols),
      show_col_types = FALSE,
      progress = TRUE,
      guess_max = 10000
    )
  },
  error = function(e) {
    message("readr::read_delim() failed: ", conditionMessage(e))
    message("Falling back to data.table::fread()...")

    if (!requireNamespace("data.table", quietly = TRUE)) {
      stop("Package 'data.table' not installed. Install with install.packages('data.table') and rerun.")
    }

    dt <- data.table::fread(in_euro_ibuy, sep = "\t", data.table = FALSE)
    names(dt) <- trimws(names(dt))

    dt_key  <- names(dt)[1]
    dt_keep <- c(dt_key, intersect(names(dt), keep_years))

    if (length(dt_keep) <= 1) {
      stop("fread() worked, but still cannot find 2012–2019 columns after trimming.")
    }

    dt[, dt_keep, drop = FALSE]
  }
)

# Clean column names so years become "2012" not "2012 "
names(euro_raw) <- trimws(names(euro_raw))
key_col <- names(euro_raw)[1]

message(
  "Eurostat loaded: nrow=", nrow(euro_raw),
  " ncol=", ncol(euro_raw),
  " | key_col=", key_col
)

message("Eurostat first few key entries:")
print(utils::head(euro_raw[[key_col]], 5))

# 3.3 Filter to EU countries BEFORE pivot_longer (critical speedup) --------
# In Eurostat TSV key, geo is the last token after the last comma
# Example: "A,I_B3_12,CB_EU_FOR,PC_IND,AT" -> geo="AT"
euro_raw <- euro_raw %>%
  dplyr::mutate(
    country = sub(".*,", "", .data[[key_col]]),
    country = trimws(country)
  ) %>%
  dplyr::filter(country %in% eu_complete)

message(
  "Eurostat after EU filter: nrow=", nrow(euro_raw),
  " | countries=", dplyr::n_distinct(euro_raw$country)
)

if (nrow(euro_raw) == 0) {
  stop("After filtering Eurostat to eu_complete, no rows remain. Check that geo codes match (e.g., AT, DE, etc.).")
}

# 3.4 Now pivot only the EU subset (small) ---------------------------------
euro_long <- euro_raw %>%
  tidyr::pivot_longer(
    cols = -c(dplyr::all_of(key_col), country),
    names_to = "year",
    values_to = "value_raw"
  ) %>%
  dplyr::mutate(
    year = as.integer(trimws(year)),
    ecommerce_share = readr::parse_number(as.character(value_raw))
  )

# "Signature" = everything except geo; ensures we keep ONE consistent series
# geo is last token, so signature is "key without trailing ,geo"
euro_parsed <- euro_long %>%
  dplyr::mutate(
    sig = sub(",[^,]+$", "", .data[[key_col]])
  ) %>%
  dplyr::select(country, year, ecommerce_share, sig)

# Pick the modal signature across EU countries (ensures a single consistent series)
sig_keep <- euro_parsed %>%
  dplyr::count(sig, name = "n") %>%
  dplyr::arrange(dplyr::desc(n)) %>%
  dplyr::slice(1) %>%
  dplyr::pull(sig)

euro_eu <- euro_parsed %>%
  dplyr::filter(sig == sig_keep) %>%
  dplyr::select(country, year, ecommerce_share) %>%
  dplyr::arrange(country, year)

# Uniqueness guard
dup_eu <- euro_eu %>% dplyr::count(country, year) %>% dplyr::filter(n > 1)
if (nrow(dup_eu) > 0) {
  stop("Eurostat EU e-commerce is not unique by country-year after filtering. Investigate key dimensions.")
}

message(
  "Eurostat EU rows: ", nrow(euro_eu),
  " | countries: ", dplyr::n_distinct(euro_eu$country),
  " | years: ", min(euro_eu$year), "-", max(euro_eu$year)
)


# ---- 4) OECD (US/CA): read CSV -----------------------------------------
message("Reading OECD e-commerce CSV...")
oecd_raw <- readr::read_csv(in_oecd, show_col_types = FALSE)

loc_name  <- intersect(c("LOCATION","Location","REF_AREA"), names(oecd_raw))[1]
time_name <- intersect(c("TIME","Time","TIME_PERIOD","Year"), names(oecd_raw))[1]
val_name  <- intersect(c("Value","OBS_VALUE","ObsValue"), names(oecd_raw))[1]
stopifnot(!is.na(loc_name), !is.na(time_name), !is.na(val_name))

oecd_usca <- oecd_raw %>%
  dplyr::transmute(
    country = dplyr::case_when(
      .data[[loc_name]] == "USA" ~ "US",
      .data[[loc_name]] == "CAN" ~ "CA",
      TRUE ~ NA_character_
    ),
    year = as.integer(.data[[time_name]]),
    ecommerce_share = as.numeric(.data[[val_name]])
  ) %>%
  dplyr::filter(!is.na(country), !is.na(year), country %in% controls) %>%
  dplyr::arrange(country, year)

dup_oecd <- oecd_usca %>% dplyr::count(country, year) %>% dplyr::filter(n > 1)
if (nrow(dup_oecd) > 0) {
  stop("OECD e-commerce is not unique by country-year. Check whether multiple series are present in the export.")
}

message("OECD US/CA rows: ", nrow(oecd_usca),
        " | years: ", min(oecd_usca$year), "-", max(oecd_usca$year))

# ---- 5) Combine + save --------------------------------------------------
ecomm <- dplyr::bind_rows(euro_eu, oecd_usca) %>%
  dplyr::arrange(country, year)

saveRDS(ecomm, out_int_rds)

ecomm_proc <- ecomm %>%
  dplyr::filter(year >= year_min, year <= year_max)

saveRDS(ecomm_proc, out_proc_rds)
readr::write_csv(ecomm_proc, out_proc_csv)

message("Saved intermediate: ", out_int_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_rds)
message("Saved processed (", year_min, "–", year_max, "): ", out_proc_csv)

# ---- 6) Coverage check --------------------------------------------------
message("Quick coverage check (processed window, non-missing e-commerce share):")
print(
  ecomm_proc %>%
    dplyr::group_by(country) %>%
    dplyr::summarise(
      year_min = min(year[!is.na(ecommerce_share)], na.rm = TRUE),
      year_max = max(year[!is.na(ecommerce_share)], na.rm = TRUE),
      n_years  = sum(!is.na(ecommerce_share)),
      .groups = "drop"
    )
)

message("Row count (processed): ", nrow(ecomm_proc))
message("Note: EU years stop at 2019 by design (Eurostat redesign from 2020).")
message("12_ecomm_import_tidy.R complete.")
