# scripts/17f_trend_augmented_did.R
# ============================================================
# Trend-augmented DiD estimation (counts only; CT vs Card)
#
# Purpose:
#   - Estimate DiD models with country-specific linear time trends
#   - Same restricted sample as 17b_baseline_did_controls.R
#   - Outcomes (counts only):
#       (1) BankShare_ct      = CT / (CT + Card)
#       (2) BankCardRatio_ct  = CT / Card
#   - Specifications:
#       (1) Trend-augmented, no controls
#       (2) + log GDP per capita
#       (3) + log GDP per capita + log(1 + broadband)
#
# Econometric specification:
#   y_ct = alpha_c + lambda_t + beta*(EU_c x Post_t) + gamma_c*t + eps_ct
#
# Notes:
#   - country-specific linear trends are implemented using fixest varying slopes:
#       country[trend_num]
#   - year FE are retained, so the trend term captures country-specific linear
#     deviations around common year effects.
#
# Sample (locked, matched to Stage 5 controlled sample):
#   - Years: 2012–2023
#   - EU: complete baseline coverage countries excluding DK/LU/MT
#   - Controls: US + Canada
#
# Inference:
#   - Clustered SEs at country level
#   - Wild cluster bootstrap p-values (Webb weights)
#
# Outputs:
#   - outputs/tables/tab17f_trend_augmented_did_raw.csv
#   - outputs/tables/tab17f_trend_augmented_did_latex.tex
#   - outputs/figures/Fig17f_beta_stability_trend_augmented.png
#   - outputs/logs/17f_trend_augmented_did.log
# ============================================================

rm(list = ls())
source(here::here("scripts/00_setup.R"))

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# -----------------------------
# 0) Logging
# -----------------------------
while (sink.number() > 0) sink()

log_file <- here::here("outputs/logs/17f_trend_augmented_did.log")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("17f_trend_augmented_did.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Package checks
# -----------------------------
print_section("1. Package checks")

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install it with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install it with install.packages('fwildclusterboot').")
}
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  stop("Package 'ggplot2' is required. Install it with install.packages('ggplot2').")
}

# -----------------------------
# 2) Paths & settings
# -----------------------------
print_section("2. Paths & settings")

# Use the controls-merged panel so the sample matches 17b exactly
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_controls_country_year.rds")

out_raw <- here::here("outputs/tables/tab17f_trend_augmented_did_raw.csv")
out_tex <- here::here("outputs/tables/tab17f_trend_augmented_did_latex.tex")
out_fig <- here::here("outputs/figures/Fig17f_beta_stability_trend_augmented.png")

year_min  <- 2012L
year_max  <- 2023L
post_year <- 2018L

B_boot <- 999

cat("Input panel:\n - ", in_panel_rds, "\n\n", sep = "")
cat("Outputs:\n")
cat(" - ", out_raw, "\n", sep = "")
cat(" - ", out_tex, "\n", sep = "")
cat(" - ", out_fig, "\n", sep = "")
cat(" - ", log_file, "\n\n", sep = "")

stopifnot(file.exists(in_panel_rds))

# -----------------------------
# 3) Sample definition (LOCKED)
# -----------------------------
print_section("3. Sample definition (LOCKED)")

eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

controls <- c("US", "CA")

cat("EU treated countries (locked): ", paste(eu_complete, collapse = ", "), "\n", sep = "")
cat("Controls (locked): ", paste(controls, collapse = ", "), "\n", sep = "")
cat("NOTE: DK/LU/MT remain excluded so this script matches the Stage 5 controlled sample.\n\n")

# -----------------------------
# 4) Load panel & construct outcomes
# -----------------------------
print_section("4. Load panel & construct outcomes")

panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_count", "cp_count", "log_gdppc", "log1p_broadband")
missing_req <- setdiff(req_cols, names(panel))
if (length(missing_req) > 0) {
  stop("Missing required columns in input panel: ", paste(missing_req, collapse = ", "))
}

df <- panel %>%
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) %>%
  dplyr::mutate(
    EU   = as.integer(country %in% eu_complete),
    Post = as.integer(year >= post_year),

    # Outcome construction
    bankshare_ct     = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = dplyr::if_else(cp_count > 0, ct_sent_count / cp_count, NA_real_),

    # Numeric time variable for country-specific linear trends
    trend_num = year - year_min
  ) %>%
  dplyr::select(
    country, year, EU, Post, trend_num,
    bankshare_ct, bankcardratio_ct,
    log_gdppc, log1p_broadband
  ) %>%
  dplyr::arrange(country, year)

cat("Rows in estimation dataframe: ", nrow(df), "\n", sep = "")
cat("Countries in estimation dataframe: ", dplyr::n_distinct(df$country), "\n\n", sep = "")

# Guardrails
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

n_countries_total <- dplyr::n_distinct(df$country)

# Check control missingness
miss_ctrl <- df %>%
  dplyr::group_by(country, EU) %>%
  dplyr::summarise(
    n = dplyr::n(),
    miss_log_gdppc = mean(is.na(log_gdppc)),
    miss_log1p_bb  = mean(is.na(log1p_broadband)),
    .groups = "drop"
  ) %>%
  dplyr::arrange(dplyr::desc(miss_log_gdppc + miss_log1p_bb), country)

cat("Control missingness (share) by country, 2012–2023:\n")
print(miss_ctrl)
cat("\n")

# -----------------------------
# 5) Estimation function
# -----------------------------
print_section("5. Estimation function")

estimate_trend_did <- function(data, yvar, controls = NULL) {

  data$country <- as.factor(data$country)
  data$year    <- as.factor(data$year)

  rhs_terms <- c("EU:Post")

  if (!is.null(controls) && length(controls) > 0) {
    rhs_terms <- c(rhs_terms, controls)
  }

  # Add country-specific linear trends as ordinary regressors
  # Omit one reference country to avoid collinearity
  rhs_terms <- c(rhs_terms, 'i(country, trend_num, ref = "AT")')

  fml_txt <- paste0(
    yvar, " ~ ", paste(rhs_terms, collapse = " + "),
    " | country + year"
  )

  fixest::feols(
    fml  = stats::as.formula(fml_txt),
    data = data,
    vcov = ~ country
  )
}

# -----------------------------
# 6) Estimate models (2 outcomes × 3 specs)
# -----------------------------
print_section("6. Estimate models (2 outcomes × 3 specs)")

specs <- list(
  S1_trend_only        = character(0),
  S2_trend_plus_gdppc  = c("log_gdppc"),
  S3_trend_plus_gdppc_bb = c("log_gdppc", "log1p_broadband")
)

models <- list(
  BankShare_ct = lapply(specs, function(x) estimate_trend_did(df, "bankshare_ct", x)),
  BankCardRatio_ct = lapply(specs, function(x) estimate_trend_did(df, "bankcardratio_ct", x))
)

# -----------------------------
# 7) Extract clustered results
# -----------------------------
print_section("7. Extract clustered results")

extract_main <- function(model, outcome_name, spec_name, controls_label, n_countries_total) {

  term <- "EU:Post"

  b  <- coef(model)[term]
  V  <- vcov(model)
  se <- sqrt(V[term, term])

  t_stat  <- b / se
  p_clust <- 2 * stats::pnorm(abs(t_stat), lower.tail = FALSE)

  ci_lo <- b - 1.96 * se
  ci_hi <- b + 1.96 * se

  tibble::tibble(
    outcome        = outcome_name,
    spec           = spec_name,
    controls_label = controls_label,
    term           = term,
    beta           = as.numeric(b),
    se_cluster     = as.numeric(se),
    t_cluster      = as.numeric(t_stat),
    p_cluster      = as.numeric(p_clust),
    ci95_lo        = as.numeric(ci_lo),
    ci95_hi        = as.numeric(ci_hi),
    n_obs          = as.integer(stats::nobs(model)),
    n_countries    = as.integer(n_countries_total)
  )
}

controls_labels <- c(
  S1_trend_only          = "No",
  S2_trend_plus_gdppc    = "log GDPpc",
  S3_trend_plus_gdppc_bb = "log GDPpc + log(1+broadband)"
)

results <- dplyr::bind_rows(
  extract_main(models$BankShare_ct$S1_trend_only,           "BankShare_ct",     "1", controls_labels["S1_trend_only"],          n_countries_total),
  extract_main(models$BankShare_ct$S2_trend_plus_gdppc,     "BankShare_ct",     "2", controls_labels["S2_trend_plus_gdppc"],    n_countries_total),
  extract_main(models$BankShare_ct$S3_trend_plus_gdppc_bb,  "BankShare_ct",     "3", controls_labels["S3_trend_plus_gdppc_bb"], n_countries_total),

  extract_main(models$BankCardRatio_ct$S1_trend_only,          "BankCardRatio_ct", "1", controls_labels["S1_trend_only"],          n_countries_total),
  extract_main(models$BankCardRatio_ct$S2_trend_plus_gdppc,    "BankCardRatio_ct", "2", controls_labels["S2_trend_plus_gdppc"],    n_countries_total),
  extract_main(models$BankCardRatio_ct$S3_trend_plus_gdppc_bb, "BankCardRatio_ct", "3", controls_labels["S3_trend_plus_gdppc_bb"], n_countries_total)
) %>%
  dplyr::mutate(
    years = paste0(year_min, "–", year_max),
    post_definition = paste0("Post=1 if year≥", post_year),
    clusters = "country",
    fe_country = "Yes",
    fe_year = "Yes",
    country_trends = "Yes"
  )

# -----------------------------
# 8) Wild cluster bootstrap p-values
# -----------------------------
print_section("8. Wild cluster bootstrap p-values")

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

wild_pval <- function(model) {
  bt <- fwildclusterboot::boottest(
    model,
    clustid = "country",
    param   = "EU:Post",
    B       = B_boot,
    type    = "webb"
  )
  if (!is.null(bt$p_val))   return(as.numeric(bt$p_val))
  if (!is.null(bt$p.value)) return(as.numeric(bt$p.value))
  stop("Could not find p-value in boottest() result.")
}

pvals <- c(
  wild_pval(models$BankShare_ct$S1_trend_only),
  wild_pval(models$BankShare_ct$S2_trend_plus_gdppc),
  wild_pval(models$BankShare_ct$S3_trend_plus_gdppc_bb),
  wild_pval(models$BankCardRatio_ct$S1_trend_only),
  wild_pval(models$BankCardRatio_ct$S2_trend_plus_gdppc),
  wild_pval(models$BankCardRatio_ct$S3_trend_plus_gdppc_bb)
)

results <- results %>% dplyr::mutate(p_wild = pvals)

# Save raw results
readr::write_csv(results, out_raw)

# -----------------------------
# 9) Write LaTeX fragment
# -----------------------------
print_section("9. Write LaTeX fragment")

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)
}

cell <- function(outcome_name, spec_id, field) {
  results %>%
    dplyr::filter(outcome == outcome_name, spec == spec_id) %>%
    dplyr::pull({{field}})
}

b_bs  <- c(fmt_num(cell("BankShare_ct","1", beta)), fmt_num(cell("BankShare_ct","2", beta)), fmt_num(cell("BankShare_ct","3", beta)))
se_bs <- c(fmt_num(cell("BankShare_ct","1", se_cluster)), fmt_num(cell("BankShare_ct","2", se_cluster)), fmt_num(cell("BankShare_ct","3", se_cluster)))
p_bs  <- c(fmt_p(cell("BankShare_ct","1", p_wild)), fmt_p(cell("BankShare_ct","2", p_wild)), fmt_p(cell("BankShare_ct","3", p_wild)))

b_br  <- c(fmt_num(cell("BankCardRatio_ct","1", beta)), fmt_num(cell("BankCardRatio_ct","2", beta)), fmt_num(cell("BankCardRatio_ct","3", beta)))
se_br <- c(fmt_num(cell("BankCardRatio_ct","1", se_cluster)), fmt_num(cell("BankCardRatio_ct","2", se_cluster)), fmt_num(cell("BankCardRatio_ct","3", se_cluster)))
p_br  <- c(fmt_p(cell("BankCardRatio_ct","1", p_wild)), fmt_p(cell("BankCardRatio_ct","2", p_wild)), fmt_p(cell("BankCardRatio_ct","3", p_wild)))

latex_lines <- c(
  "% Auto-generated by scripts/17f_trend_augmented_did.R",
  "% Trend-augmented DiD estimates with country-specific linear trends",
  paste0(
    "EU $\\times$ Post(2018+) & \\textbf{",
    paste(b_bs, collapse = "} & \\textbf{"),
    "} & \\textbf{",
    paste(b_br, collapse = "} & \\textbf{"),
    "} \\\\"
  ),
  paste0(
    "& (",
    paste(se_bs, collapse = ") & ("),
    ") & (",
    paste(se_br, collapse = ") & ("),
    ") \\\\"
  ),
  paste0(
    "Wild bootstrap p-value & ",
    paste(p_bs, collapse = " & "),
    " & ",
    paste(p_br, collapse = " & "),
    " \\\\"
  )
)

writeLines(latex_lines, out_tex)

# -----------------------------
# 10) Save coefficient stability plot
# -----------------------------
print_section("10. Save coefficient stability plot")

plot_df <- results %>%
  dplyr::mutate(
    outcome_label = dplyr::case_when(
      outcome == "BankShare_ct" ~ "BankShare",
      outcome == "BankCardRatio_ct" ~ "BankCardRatio",
      TRUE ~ outcome
    ),
    spec_label = dplyr::case_when(
      spec == "1" ~ "Trend only",
      spec == "2" ~ "Trend + log GDPpc",
      spec == "3" ~ "Trend + log GDPpc + log(1+broadband)",
      TRUE ~ spec
    )
  )

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = spec_label, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::facet_wrap(~ outcome_label, scales = "free_y") +
  ggplot2::labs(
    title = "Trend-augmented DiD estimates (EU × Post)",
    subtitle = paste0(
      "Country and year FE + country-specific linear trends. ",
      "Clustered SE 95% CI; wild bootstrap p-values in table. B=", B_boot
    ),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12) +
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 15, hjust = 1))

ggplot2::ggsave(out_fig, p, width = 10, height = 5.5, dpi = 300)

# -----------------------------
# 11) End + console summary
# -----------------------------
print_section("11. Complete")

cat("17f_trend_augmented_did.R complete.\n\n")
cat("Saved outputs:\n")
cat(" - Raw results:   ", out_raw, "\n", sep = "")
cat(" - LaTeX fragment:", out_tex, "\n", sep = "")
cat(" - Stability plot:", out_fig, "\n\n", sep = "")

cat("Key coefficients (EU x Post) with wild bootstrap p-values:\n")
print(results %>% dplyr::select(outcome, spec, controls_label, beta, se_cluster, p_wild, n_obs, n_countries))
cat("\n")

cat("\n=========================================\n")
cat("17f_trend_augmented_did.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n")