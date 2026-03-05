# ============================================================
# Baseline card exposure (2015)
# Conceptual heterogeneity input (DDD)
#
# Purpose:
#   - Construct a baseline measure of card-payment exposure prior to PSD2.
#   - This variable is used for a conceptual heterogeneity extension
#     (difference-in-difference-in-differences, DDD).
#
# Definition:
#   card_exposure_2015 = Card_count / (Card_count + CT_count)
#
# Interpretation:
#   - Higher values indicate greater reliance on card payments relative
#     to credit transfers prior to PSD2.
#   - Countries are classified into:
#         "High card exposure"  (≥ median exposure)
#         "Low card exposure"   (< median exposure)
#
# Sample:
#   - Year: 2015 (pre-treatment baseline)
#   - Countries: EU member states with complete baseline coverage
#
# Note:
#   This script does NOT estimate a DDD model. It only constructs the
#   baseline exposure variable that could be used in a future
#   heterogeneous-treatment specification.
#
# Output:
#   outputs/tables/tab18b_baseline_card_exposure_2015.csv
# ============================================================

source(here::here("scripts/00_setup.R"))

in_panel <- here::here(
  "data/processed/panel/master_panel_payments_country_year.rds"
)

out_tab <- here::here(
  "outputs/tables/tab18b_baseline_card_exposure_2015.csv"
)

eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

panel <- readRDS(in_panel)

exposure_2015 <- panel %>%
  dplyr::filter(
    year == 2015,
    country %in% eu_complete
  ) %>%
  dplyr::mutate(
    card_exposure_2015 = cp_count / (cp_count + ct_sent_count)
  ) %>%
  dplyr::select(country, card_exposure_2015) %>%
  dplyr::filter(is.finite(card_exposure_2015))

median_exposure <- median(
  exposure_2015$card_exposure_2015,
  na.rm = TRUE
)

exposure_2015 <- exposure_2015 %>%
  dplyr::mutate(
    exposure_group = ifelse(
      card_exposure_2015 >= median_exposure,
      "High card exposure",
      "Low card exposure"
    )
  ) %>%
  dplyr::arrange(desc(card_exposure_2015))

# Overleaf-safe CSV (NO commas inside cells)
readr::write_csv(exposure_2015, out_tab)

message("Baseline exposure table saved:")
message(" - ", out_tab)
