# scripts/16_descriptives_and_trends.R
# ============================================================
# Descriptives & pre-trends diagnostics (counts only, baseline outcomes)
#
# Purpose:
#   - Assess plausibility of parallel trends BEFORE running DiD regressions.
#   - Pre-period only: 2012–2017.
#
# Design choices (baseline):
#   - Outcomes: based on counts only; exclude DD and instant payments.
#   - EU sample: only EU countries with complete baseline coverage (CT & Card counts).
#   - Controls: United States + Canada (BIS).
#
# Outputs:
#   - 4 figures:
#       (1) BankShare levels (EU vs controls)
#       (2) BankCardRatio levels (EU vs controls)
#       (3) BankShare indexed to 2012=100
#       (4) BankCardRatio indexed to 2012=100
#   - Table of group means used for plotting
# ============================================================

source(here::here("scripts/00_setup.R"))

# ---- 1) Paths & settings -----------------------------------------------

in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

# Pre-trends window (pre-PSD2 only)
year_min <- 2012L
year_max <- 2017L

# Output locations
fig_dir <- here::here("outputs/figures")
tab_dir <- here::here("outputs/tables")
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

# File names (keep stable so you can reference them in the thesis)
f1 <- here::here(fig_dir, "Fig16_1_pretrends_levels_bankshare_ct.png")
f2 <- here::here(fig_dir, "Fig16_2_pretrends_levels_bankcardratio_ct.png")
f3 <- here::here(fig_dir, "Fig16_3_pretrends_indexed_bankshare_ct.png")
f4 <- here::here(fig_dir, "Fig16_4_pretrends_indexed_bankcardratio_ct.png")

out_means <- here::here(tab_dir, "tab16_group_means_pretrends_2012_2017.csv")

# ---- 2) Load master panel ----------------------------------------------

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

# Basic checks: required columns
req_cols <- c("country", "year", "is_eu", "source_group", "ct_sent_count", "cp_count")
stopifnot(all(req_cols %in% names(panel)))

# ---- 3) Define analysis sample (EU complete coverage + US/CA controls) --

# Note:
# We restrict EU countries to those with complete baseline coverage in the BIS window.
# This decision is based on the exported coverage table from script 14:
#   outputs/tables/coverage_master_panel_payments.csv
#
# Countries excluded under this rule: MT, DK, LU (in your current data).

# EU countries with complete baseline coverage (ct_sent_count & cp_count observed for all years 2012–2023)
# Here we implement the conclusion (rather than recomputing it) to keep this script focused.
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

# Controls (BIS)
controls <- c("US", "CA")

sample_panel <- panel %>%
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) %>%
  dplyr::mutate(
    group = dplyr::case_when(
      country %in% eu_complete ~ "EU",
      country %in% controls ~ "Control",
      TRUE ~ NA_character_
    )
  ) %>%
  dplyr::filter(!is.na(group))

# Guardrails: confirm sample structure
stopifnot(all(unique(sample_panel$country[sample_panel$group == "Control"]) %in% controls))

# ---- 4) Construct baseline outcomes (counts only, no DD) ----------------

# BankShare_ct = CT / (CT + Card)
# BankCardRatio_ct = CT / Card
# Note: We require cp_count > 0 for the ratio to be well-defined.
sample_panel <- sample_panel %>%
  dplyr::mutate(
    bankshare_ct = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = dplyr::if_else(cp_count > 0, ct_sent_count / cp_count, NA_real_)
  )

# ---- 5) Aggregate to group means (EU unweighted; controls average US/CA) -

# EU mean: simple average across EU countries each year (unweighted)
# Control mean: simple average of US and CA each year (unweighted)
group_means <- sample_panel %>%
  dplyr::group_by(group, year) %>%
  dplyr::summarise(
    bankshare_ct = mean(bankshare_ct, na.rm = TRUE),
    bankcardratio_ct = mean(bankcardratio_ct, na.rm = TRUE),
    n_countries = dplyr::n_distinct(country),
    .groups = "drop"
  ) %>%
  dplyr::arrange(group, year)

# Save the means (useful for appendix/reproducibility)
readr::write_csv(group_means, out_means)

# ---- 6) Build indexed series (2012 = 100) --------------------------------

# Index within each group and outcome
group_means_indexed <- group_means %>%
  dplyr::group_by(group) %>%
  dplyr::mutate(
    bankshare_ct_index = (bankshare_ct / bankshare_ct[year == year_min]) * 100,
    bankcardratio_ct_index = (bankcardratio_ct / bankcardratio_ct[year == year_min]) * 100
  ) %>%
  dplyr::ungroup()

# ---- 7) Plotting helper -------------------------------------------------

save_plot <- function(p, path, w = 8, h = 5, dpi = 300) {
  ggplot2::ggsave(filename = path, plot = p, width = w, height = h, dpi = dpi)
}

# ---- 8) Figure 1: BankShare levels --------------------------------------

p1 <- ggplot2::ggplot(group_means, ggplot2::aes(x = year, y = bankshare_ct, linetype = group)) +
  ggplot2::geom_line(linewidth = 1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  ggplot2::labs(
    title = "Pre-trends (2012–2017): BankShare (CT / (CT + Card))",
    subtitle = "EU mean (unweighted) vs average of US and Canada",
    x = NULL, y = "BankShare (counts)",
    linetype = NULL
  ) +
  ggplot2::theme_minimal(base_size = 12)

save_plot(p1, f1)

# ---- 9) Figure 2: BankCardRatio levels ----------------------------------

p2 <- ggplot2::ggplot(group_means, ggplot2::aes(x = year, y = bankcardratio_ct, linetype = group)) +
  ggplot2::geom_line(linewidth = 1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::labs(
    title = "Pre-trends (2012–2017): BankCardRatio (CT / Card)",
    subtitle = "EU mean (unweighted) vs average of US and Canada",
    x = NULL, y = "CT / Card (counts)",
    linetype = NULL
  ) +
  ggplot2::theme_minimal(base_size = 12)

save_plot(p2, f2)

# ---- 10) Figure 3: BankShare indexed (2012 = 100) -----------------------

p3 <- ggplot2::ggplot(group_means_indexed, ggplot2::aes(x = year, y = bankshare_ct_index, linetype = group)) +
  ggplot2::geom_line(linewidth = 1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::labs(
    title = "Pre-trends (2012–2017): BankShare indexed to 2012 = 100",
    subtitle = "Growth-rate style comparison (levels normalized)",
    x = NULL, y = "Index (2012 = 100)",
    linetype = NULL
  ) +
  ggplot2::theme_minimal(base_size = 12)

save_plot(p3, f3)

# ---- 11) Figure 4: BankCardRatio indexed (2012 = 100) -------------------

p4 <- ggplot2::ggplot(group_means_indexed, ggplot2::aes(x = year, y = bankcardratio_ct_index, linetype = group)) +
  ggplot2::geom_line(linewidth = 1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::labs(
    title = "Pre-trends (2012–2017): BankCardRatio indexed to 2012 = 100",
    subtitle = "Growth-rate style comparison (levels normalized)",
    x = NULL, y = "Index (2012 = 100)",
    linetype = NULL
  ) +
  ggplot2::theme_minimal(base_size = 12)

save_plot(p4, f4)

# ---- 12) Console summary (quick sanity output) --------------------------

message("16_descriptives_and_trends.R complete.")
message("Saved figures:")
message(" - ", f1)
message(" - ", f2)
message(" - ", f3)
message(" - ", f4)
message("Saved table: ", out_means)
