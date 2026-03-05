# scripts/18_event_study.R
# ============================================================
# Event-study (diagnostic): EU treated vs US/CA controls
# Counts only, CT vs Card outcomes, no controls in regression.
#
# Purpose (Stage 4):
#   - Visual check of dynamic parallel trends:
#       * pre-treatment leads should be ~0 (flat)
#       * post-treatment lags show dynamic responses
#
# Sample (LOCKED to match baseline):
#   - Years: 2012–2023
#   - EU: complete baseline coverage countries (24; excludes DK/LU/MT)
#   - Controls: US + Canada
#
# Model (TWFE event study):
#   y_ct = alpha_c + lambda_t + sum_{k != -1} beta_k [EU_c * 1{event_time=k}] + eps_ct
#   - event_time = year - 2018
#   - reference period: k = -1 (2017)
#   - window: -6 to +5  (2012–2017 leads, 2018–2023 lags)
#
# Inference:
#   - Cluster-robust SE at country level
#
# Outputs:
#   - outputs/tables/tab18_eventstudy_coefs.csv
#   - outputs/tables/tab18_eventstudy_coefs_slim.csv
#   - outputs/figures/Fig18_1_eventstudy_bankshare_ct.png
#   - outputs/figures/Fig18_2_eventstudy_bankcardratio_ct.png
#   - outputs/logs/18_event_study.log
# ============================================================

rm(list = ls())
source(here::here("scripts/00_setup.R"))

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# ---- 0) Logging ---------------------------------------------------------
while (sink.number() > 0) sink()

log_file <- here::here("outputs/logs/18_event_study.log")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("============================================================\n")
cat("18_event_study.R\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("============================================================\n\n")

# ---- 1) Packages --------------------------------------------------------
print_section("1. Package checks")

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install it with install.packages('fixest').")
}
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  stop("Package 'ggplot2' is required. Install it with install.packages('ggplot2').")
}
if (!requireNamespace("dplyr", quietly = TRUE)) {
  stop("Package 'dplyr' is required. Install it with install.packages('dplyr').")
}
if (!requireNamespace("tibble", quietly = TRUE)) {
  stop("Package 'tibble' is required. Install it with install.packages('tibble').")
}
if (!requireNamespace("readr", quietly = TRUE)) {
  stop("Package 'readr' is required. Install it with install.packages('readr').")
}
if (!requireNamespace("stringr", quietly = TRUE)) {
  stop("Package 'stringr' is required. Install it with install.packages('stringr').")
}

# ---- 2) Paths & settings ------------------------------------------------
print_section("2. Paths & settings")

in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

out_tab      <- here::here("outputs/tables/tab18_eventstudy_coefs.csv")
out_tab_slim <- here::here("outputs/tables/tab18_eventstudy_coefs_slim.csv")
out_fig1     <- here::here("outputs/figures/Fig18_1_eventstudy_bankshare_ct.png")
out_fig2     <- here::here("outputs/figures/Fig18_2_eventstudy_bankcardratio_ct.png")

dir.create(dirname(out_tab), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_fig1), recursive = TRUE, showWarnings = FALSE)

year_min   <- 2012L
year_max   <- 2023L
treat_year <- 2018L

k_min <- -6L
k_max <-  5L
k_ref <- -1L

# Locked sample lists (aligned with baseline scripts; DK/LU/MT excluded on purpose)
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)
controls <- c("US", "CA")

cat("Input panel:\n - ", in_panel_rds, "\n\n", sep = "")
cat("Outputs:\n")
cat(" - ", out_tab, "\n", sep = "")
cat(" - ", out_tab_slim, "\n", sep = "")
cat(" - ", out_fig1, "\n", sep = "")
cat(" - ", out_fig2, "\n\n", sep = "")
cat("Window: years ", year_min, "–", year_max, " | treat_year=", treat_year,
    " | k in [", k_min, ",", k_max, "] | ref=", k_ref, "\n\n", sep = "")

# ---- 3) Helpers ---------------------------------------------------------
print_section("3. Helpers")

assert_unique_key <- function(df, key_vars = c("country", "year"), df_name = "data") {
  dup_n <- df |>
    dplyr::count(dplyr::across(dplyr::all_of(key_vars))) |>
    dplyr::filter(n > 1) |>
    nrow()
  if (dup_n > 0) stop("Duplicate country-year rows detected in ", df_name, ".")
  invisible(TRUE)
}

extract_event_coefs <- function(model, outcome_name, event_var = "event_time") {
  cf <- stats::coef(model)
  V  <- stats::vcov(model)  # <-- IMPORTANT: do NOT use fixest::vcov()

  terms <- names(cf)

  keep <- stringr::str_detect(terms, event_var)
  terms_k <- terms[keep]

  if (length(terms_k) == 0) {
    cat("\nDEBUG: first 80 coefficient names:\n")
    print(utils::head(terms, 80))
    stop(
      "No event-study terms found. The model estimated, but coefficient names did not match.\n",
      "See printed DEBUG list above and adjust the keep-rule if needed."
    )
  }

  k <- vapply(terms_k, function(tt) {
    nums <- stringr::str_extract_all(tt, "-?\\d+")[[1]]
    if (length(nums) == 0) return(NA_integer_)
    suppressWarnings(as.integer(tail(nums, 1)))
  }, integer(1))

  se <- vapply(terms_k, function(tt) sqrt(V[tt, tt]), numeric(1))

  tibble::tibble(
    outcome = outcome_name,
    term = terms_k,
    event_time = k,
    beta = as.numeric(cf[terms_k]),
    se_cluster = as.numeric(se)
  ) |>
    dplyr::filter(!is.na(event_time)) |>
    dplyr::mutate(
      ci95_lo = beta - 1.96 * se_cluster,
      ci95_hi = beta + 1.96 * se_cluster
    ) |>
    dplyr::arrange(event_time)
}

plot_eventstudy <- function(df_es, title, out_path) {
  p <- ggplot2::ggplot(df_es, ggplot2::aes(x = event_time, y = beta)) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
    ggplot2::geom_vline(xintercept = 0, linetype = "dotted") +
    ggplot2::geom_point(size = 2) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.2) +
    ggplot2::scale_x_continuous(breaks = seq(k_min, k_max, by = 1)) +
    ggplot2::labs(
      title = title,
      subtitle = paste0(
        "Event-time relative to ", treat_year, " (0=", treat_year, "). Reference: k=", k_ref,
        " (", treat_year + k_ref, "). Clustered SE: country."
      ),
      x = "Event time (k = year − 2018)",
      y = "Estimate (β_k)"
    ) +
    ggplot2::theme_minimal(base_size = 12)

  ggplot2::ggsave(out_path, p, width = 8, height = 5, dpi = 300)
  invisible(p)
}

# ---- 4) Load panel + build analysis dataset -----------------------------
print_section("4. Load panel & construct outcomes")

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_count", "cp_count")
stopifnot(all(req_cols %in% names(panel)))

panel <- panel |>
  dplyr::mutate(
    country = as.character(country),
    year = as.integer(year)
  )

assert_unique_key(panel, df_name = "master_panel_payments")

df <- panel |>
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) |>
  dplyr::mutate(
    EU = as.integer(country %in% eu_complete),
    event_time = year - treat_year,
    bankshare_ct = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = dplyr::if_else(cp_count > 0, ct_sent_count / cp_count, NA_real_)
  ) |>
  dplyr::filter(event_time >= k_min, event_time <= k_max) |>
  dplyr::select(country, year, EU, event_time, bankshare_ct, bankcardratio_ct) |>
  dplyr::arrange(country, year)

cat("Sample summary:\n")
cat("Rows: ", nrow(df), "\n", sep = "")
cat("Countries: ", dplyr::n_distinct(df$country), "\n", sep = "")
cat("Years: ", min(df$year), " - ", max(df$year), "\n", sep = "")
cat("Event-time window: ", k_min, " to ", k_max, " (ref=", k_ref, ")\n\n", sep = "")

stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))
stopifnot(any(df$event_time == k_ref))

# ---- 5) Estimate event-study models ------------------------------------
print_section("5. Estimate event-study models")

estimate_event <- function(data, yvar) {
  data$country <- as.factor(data$country)
  data$year    <- as.factor(data$year)

  fml <- stats::as.formula(
    paste0(yvar, " ~ i(event_time, EU, ref = ", k_ref, ") | country + year")
  )

  fixest::feols(
    fml  = fml,
    data = data,
    vcov = ~ country
  )
}

m_bs <- estimate_event(df, "bankshare_ct")
m_br <- estimate_event(df, "bankcardratio_ct")

cat("Models estimated.\n\n")

# ---- 6) Extract coefficients + save tables ------------------------------
print_section("6. Extract coefficients & save tables")

es_bs <- extract_event_coefs(m_bs, "BankShare_ct")
es_br <- extract_event_coefs(m_br, "BankCardRatio_ct")

es_all <- dplyr::bind_rows(es_bs, es_br) |>
  dplyr::mutate(
    year = treat_year + event_time,
    window = paste0("[", k_min, ",", k_max, "]"),
    ref = k_ref,
    sample_years = paste0(year_min, "–", year_max),
    treated = "EU complete-coverage (24)",
    controls = "US+CA",
    se = "Cluster(country)"
  )

readr::write_csv(es_all, out_tab)
cat("Saved table:\n - ", out_tab, "\n\n", sep = "")

es_slim <- es_all |>
  dplyr::transmute(
    outcome = outcome,
    k       = event_time,
    beta    = beta,
    se      = se_cluster,
    ci_lo   = ci95_lo,
    ci_hi   = ci95_hi
  )

readr::write_csv(es_slim, out_tab_slim)
cat("Saved Overleaf-safe table:\n - ", out_tab_slim, "\n\n", sep = "")

# ---- 7) Plots -----------------------------------------------------------
print_section("7. Save plots")

plot_eventstudy(
  df_es = es_bs,
  title = "Event-study: BankShare (counts-based)",
  out_path = out_fig1
)
plot_eventstudy(
  df_es = es_br,
  title = "Event-study: BankCardRatio (counts-based)",
  out_path = out_fig2
)

cat("Saved figures:\n")
cat(" - ", out_fig1, "\n", sep = "")
cat(" - ", out_fig2, "\n\n", sep = "")

cat("18_event_study.R complete.\n")


