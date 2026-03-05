# scripts/16b_raw_means_diagnostics.R
# ============================================================
# RAW MEANS DIAGNOSTICS (NO ESTIMATION)
# Stage 5/7 add-on (descriptive packaging only)
#
# Purpose:
#   Transparent raw-mean diagnostics to help interpret the (near-)null DiD results.
#   Purely descriptive: NO regressions, NO FE, NO clustering, NO bootstrap.
#
# Data:
#   - Payments-only master panel (country-year)
#   - Counts-based outcomes (baseline definitions)
#
# Sample (LOCKED to match baseline DiD sample):
#   - Years: 2012–2023
#   - Treated: EU (24-country list; excludes DK, LU, MT)
#   - Controls: US + CA
#
# Outcomes (counts-based):
#   - BankShare_ct     = CT / (CT + Card)
#   - BankCardRatio_ct = CT / Card
#
# Figures:
#   Fig16b_1_raw_means_bankshare.png
#   Fig16b_2_raw_means_bankcardratio.png
#   Fig16b_3_eu_country_trajectories_bankshare.png
#   Fig16b_4_country_changes_bankshare_ct.png
#   Fig16b_5_country_changes_bankcardratio_ct.png
#
# Tables:
#   tab16b_raw_means_by_group.csv
#   tab16b_country_changes_pre_post.csv
# ============================================================

rm(list = ls())
source(here::here("scripts/00_setup.R"))

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# --- Small helper: wrap long subtitles to avoid clipping/truncation
wrap_subtitle <- function(x, width = 95) {
  if (!requireNamespace("stringr", quietly = TRUE)) return(x)
  stringr::str_wrap(x, width = width)
}

# --- Common theme tweak: add a bit of top/right margin so multi-line subtitles fit
theme_diag <- function(base_size = 12) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      legend.title = ggplot2::element_blank(),
      plot.title.position = "plot",
      plot.caption.position = "plot",
      plot.margin = ggplot2::margin(t = 12, r = 18, b = 10, l = 10)
    )
}

# -----------------------------
# 1) Paths & settings
# -----------------------------
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

out_fig1 <- here::here("outputs/figures/Fig16b_1_raw_means_bankshare.png")
out_fig2 <- here::here("outputs/figures/Fig16b_2_raw_means_bankcardratio.png")
out_fig3 <- here::here("outputs/figures/Fig16b_3_eu_country_trajectories_bankshare.png")
out_fig4 <- here::here("outputs/figures/Fig16b_4_country_changes_bankshare_ct.png")
out_fig5 <- here::here("outputs/figures/Fig16b_5_country_changes_bankcardratio_ct.png")

out_tab1 <- here::here("outputs/tables/tab16b_raw_means_by_group.csv")
out_tab2 <- here::here("outputs/tables/tab16b_country_changes_pre_post.csv")

dir.create(dirname(out_fig1), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_tab1), recursive = TRUE, showWarnings = FALSE)

year_min <- 2012L
year_max <- 2023L
policy_year <- 2018L

pre_min <- 2012L
pre_max <- 2017L
post_min <- 2018L
post_max <- 2023L

# -----------------------------
# 2) Sample definition (LOCKED)
# -----------------------------
eu_complete_24 <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)
controls <- c("US", "CA")

# -----------------------------
# 3) Load panel & construct outcomes
# -----------------------------
print_section("3. Load panel & construct outcomes (counts-based)")

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_count", "cp_count")
missing_cols <- setdiff(req_cols, names(panel))
if (length(missing_cols) > 0) {
  stop("Missing required columns in master panel: ", paste(missing_cols, collapse = ", "))
}

df <- panel |>
  dplyr::mutate(
    country = as.character(country),
    year    = as.integer(year)
  ) |>
  dplyr::filter(
    year >= year_min, year <= year_max,
    country %in% c(eu_complete_24, controls)
  ) |>
  dplyr::mutate(
    group = dplyr::case_when(
      country %in% eu_complete_24 ~ "EU",
      country %in% controls       ~ "Control",
      TRUE                        ~ NA_character_
    ),
    bankshare_ct = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = ct_sent_count / cp_count
  ) |>
  dplyr::select(country, year, group, bankshare_ct, bankcardratio_ct) |>
  dplyr::arrange(country, year)

# Guardrails
stopifnot(all(unique(df$country[df$group == "Control"]) %in% controls))
stopifnot(all(unique(df$country[df$group == "EU"]) %in% eu_complete_24))

cat("Rows: ", nrow(df), "\n", sep = "")
cat("Countries: ", dplyr::n_distinct(df$country), "\n", sep = "")
cat("EU countries: ", length(eu_complete_24), "\n", sep = "")
cat("Controls: ", paste(controls, collapse = ", "), "\n\n", sep = "")

# -----------------------------
# 4) Figure 1: Raw means EU vs Control (levels)
# -----------------------------
print_section("4. Figure 1: Raw means EU vs Control (levels)")

means_by_group <- df |>
  dplyr::group_by(year, group) |>
  dplyr::summarise(
    bankshare_mean = mean(bankshare_ct, na.rm = TRUE),
    bankcardratio_mean = mean(bankcardratio_ct, na.rm = TRUE),
    n_countries = dplyr::n_distinct(country[is.finite(bankshare_ct) | is.finite(bankcardratio_ct)]),
    .groups = "drop"
  ) |>
  dplyr::arrange(group, year)

# Save table (Overleaf-safe CSV)
readr::write_csv(means_by_group, out_tab1)

subtitle_group_means <- wrap_subtitle(
  "EU mean (unweighted across EU countries) vs Control mean (unweighted across US & Canada). Dashed line: 2018.",
  width = 95
)

# BankShare plot
p1 <- ggplot2::ggplot(
  means_by_group,
  ggplot2::aes(x = year, y = bankshare_mean, linetype = group, group = group)
) +
  ggplot2::geom_vline(xintercept = policy_year, linetype = "dashed") +
  ggplot2::geom_line(linewidth = 0.9, na.rm = TRUE) +
  ggplot2::geom_point(size = 2, na.rm = TRUE) +
  ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  ggplot2::scale_x_continuous(breaks = seq(year_min, year_max, by = 2)) +
  ggplot2::labs(
    title = "Raw means (2012–2023): BankShare = CT / (CT + Card)",
    subtitle = subtitle_group_means,
    x = NULL,
    y = "BankShare (counts)"
  ) +
  theme_diag(base_size = 12)

ggplot2::ggsave(out_fig1, p1, width = 7.8, height = 4.6, dpi = 300)

# BankCardRatio plot
p2 <- ggplot2::ggplot(
  means_by_group,
  ggplot2::aes(x = year, y = bankcardratio_mean, linetype = group, group = group)
) +
  ggplot2::geom_vline(xintercept = policy_year, linetype = "dashed") +
  ggplot2::geom_line(linewidth = 0.9, na.rm = TRUE) +
  ggplot2::geom_point(size = 2, na.rm = TRUE) +
  ggplot2::scale_x_continuous(breaks = seq(year_min, year_max, by = 2)) +
  ggplot2::labs(
    title = "Raw means (2012–2023): BankCardRatio = CT / Card",
    subtitle = subtitle_group_means,
    x = NULL,
    y = "CT / Card (counts)"
  ) +
  theme_diag(base_size = 12)

ggplot2::ggsave(out_fig2, p2, width = 7.8, height = 4.6, dpi = 300)

# -----------------------------
# 5) Figure 2: EU country trajectories (spaghetti) — BankShare only
# -----------------------------
print_section("5. Figure 2: EU country trajectories (spaghetti) — BankShare")

df_eu <- df |>
  dplyr::filter(group == "EU") |>
  dplyr::filter(is.finite(bankshare_ct))

eu_avg <- df_eu |>
  dplyr::group_by(year) |>
  dplyr::summarise(bankshare_mean = mean(bankshare_ct, na.rm = TRUE), .groups = "drop")

p3 <- ggplot2::ggplot(df_eu, ggplot2::aes(x = year, y = bankshare_ct, group = country)) +
  ggplot2::geom_vline(xintercept = policy_year, linetype = "dashed") +
  ggplot2::geom_line(alpha = 0.35, linewidth = 0.7) +
  ggplot2::geom_line(
    data = eu_avg,
    ggplot2::aes(x = year, y = bankshare_mean, group = 1),
    linewidth = 1.2
  ) +
  ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  ggplot2::scale_x_continuous(breaks = seq(year_min, year_max, by = 2)) +
  ggplot2::labs(
    title = "EU country trajectories (2012–2023): BankShare",
    subtitle = wrap_subtitle(
      "Thin lines: individual EU countries. Thick line: EU unweighted mean. Dashed line: 2018.",
      width = 95
    ),
    x = NULL,
    y = "BankShare (counts)"
  ) +
  theme_diag(base_size = 12)

ggplot2::ggsave(out_fig3, p3, width = 7.8, height = 4.9, dpi = 300)

# -----------------------------
# 6) Country-level raw change plots (post mean − pre mean)
# -----------------------------
print_section("6. NEW: Country-level raw change plots (post − pre)")

df_changes <- df |>
  dplyr::mutate(
    period = dplyr::case_when(
      year >= pre_min  & year <= pre_max  ~ "pre",
      year >= post_min & year <= post_max ~ "post",
      TRUE ~ NA_character_
    )
  ) |>
  dplyr::filter(!is.na(period)) |>
  dplyr::group_by(country, group, period) |>
  dplyr::summarise(
    bankshare_mean = mean(bankshare_ct, na.rm = TRUE),
    bankcardratio_mean = mean(bankcardratio_ct, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_wider(
    names_from = period,
    values_from = c(bankshare_mean, bankcardratio_mean)
  ) |>
  dplyr::mutate(
    delta_bankshare_ct = bankshare_mean_post - bankshare_mean_pre,
    delta_bankcardratio_ct = bankcardratio_mean_post - bankcardratio_mean_pre
  )

# Save table for appendix/reproducibility
readr::write_csv(df_changes, out_tab2)

# Ordering: EU sorted by delta, then US/CA appended (as reference markers)
eu_order_bs <- df_changes |>
  dplyr::filter(group == "EU") |>
  dplyr::arrange(delta_bankshare_ct) |>
  dplyr::pull(country)

levels_bs <- c(eu_order_bs, controls)

plot_changes_bs <- df_changes |>
  dplyr::filter(country %in% levels_bs) |>
  dplyr::mutate(country_f = factor(country, levels = levels_bs))

# Force a clean multi-line subtitle (avoids any clipping)
subtitle_changes <- paste0(
  "Δy_c = mean(2018–2023) − mean(2012–2017). EU bars sorted.\n",
  "US/CA shown as reference markers."
)

p4 <- ggplot2::ggplot(plot_changes_bs, ggplot2::aes(x = country_f, y = delta_bankshare_ct)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_col(
    data = dplyr::filter(plot_changes_bs, group == "EU"),
    fill = "grey70",
    width = 0.8
  ) +
  ggplot2::geom_point(
    data = dplyr::filter(plot_changes_bs, group == "Control"),
    shape = 21, fill = "white", size = 2.8, stroke = 1
  ) +
  ggplot2::coord_flip() +
  ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  ggplot2::labs(
    title = "Country raw change (post − pre): BankShare (counts)",
    subtitle = subtitle_changes,
    x = NULL,
    y = "Δ BankShare (percentage points)"
  ) +
  theme_diag(base_size = 12)

# Slightly wider canvas for long title/subtitle + many country labels
ggplot2::ggsave(out_fig4, p4, width = 8.6, height = 6.0, dpi = 300)

# Same for BankCardRatio
eu_order_bcr <- df_changes |>
  dplyr::filter(group == "EU") |>
  dplyr::arrange(delta_bankcardratio_ct) |>
  dplyr::pull(country)

levels_bcr <- c(eu_order_bcr, controls)

plot_changes_bcr <- df_changes |>
  dplyr::filter(country %in% levels_bcr) |>
  dplyr::mutate(country_f = factor(country, levels = levels_bcr))

p5 <- ggplot2::ggplot(plot_changes_bcr, ggplot2::aes(x = country_f, y = delta_bankcardratio_ct)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_col(
    data = dplyr::filter(plot_changes_bcr, group == "EU"),
    fill = "grey70",
    width = 0.8
  ) +
  ggplot2::geom_point(
    data = dplyr::filter(plot_changes_bcr, group == "Control"),
    shape = 21, fill = "white", size = 2.8, stroke = 1
  ) +
  ggplot2::coord_flip() +
  ggplot2::labs(
    title = "Country raw change (post − pre): BankCardRatio (counts)",
    subtitle = subtitle_changes,
    x = NULL,
    y = "Δ (CT / Card)"
  ) +
  theme_diag(base_size = 12)

ggplot2::ggsave(out_fig5, p5, width = 8.6, height = 6.0, dpi = 300)

# -----------------------------
# 7) Done
# -----------------------------
cat("\n16b_raw_means_diagnostics.R complete.\n")
cat("Saved figures:\n")
cat(" - ", out_fig1, "\n", sep = "")
cat(" - ", out_fig2, "\n", sep = "")
cat(" - ", out_fig3, "\n", sep = "")
cat(" - ", out_fig4, "\n", sep = "")
cat(" - ", out_fig5, "\n", sep = "")
cat("Saved tables:\n")
cat(" - ", out_tab1, "\n", sep = "")
cat(" - ", out_tab2, "\n", sep = "")

