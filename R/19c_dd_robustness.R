# scripts/19c_dd_robustness.R
# ============================================================
# Direct debits robustness (minimal)
# Purpose (Stage 7):
#   - Robustness to alternative definition of bank-based payments:
#       BankShare^(CT+DD)_ct = (CT + DD) / (CT + DD + Card)
#   - Restrict to late window: 2022–2024
#   - One TWFE DiD spec (EU × Post), no additional controls
#   - Inference: country-clustered SEs + wild cluster bootstrap p-value
#
# Sample:
#   - Years: 2022–2024
#   - Post: 1{year >= 2023}
#   - EU treated: eu_complete locked list
#   - Controls: US + CA (baseline controls)
#
# Outputs:
#   - outputs/tables/tab19c_dd_robustness_raw.csv
#   - outputs/tables/tab19c_dd_robustness_latex.tex
#   - outputs/figures/Fig19c_dd_robustness_beta.png
#   - outputs/logs/19c_dd_robustness_log.txt
# ============================================================

rm(list = ls())
source(here::here("scripts/00_setup.R"))

print_section <- function(title) {
  cat("\n=========================================\n")
  cat(title, "\n")
  cat("=========================================\n")
}

# -----------------------------
# 0) Logging (simple + safe)
# -----------------------------
log_file <- here::here("outputs/logs", "19c_dd_robustness_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)

on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n============================================================\n")
cat("19c_dd_robustness.R\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("============================================================\n\n")

# -----------------------------
# 1) Packages
# -----------------------------
if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install it with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install it with install.packages('fwildclusterboot').")
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
if (!requireNamespace("ggplot2", quietly = TRUE)) {
  stop("Package 'ggplot2' is required. Install it with install.packages('ggplot2').")
}

# -----------------------------
# 2) Paths & settings
# -----------------------------
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

out_raw <- here::here("outputs/tables/tab19c_dd_robustness_raw.csv")
out_tex <- here::here("outputs/tables/tab19c_dd_robustness_latex.tex")
out_fig <- here::here("outputs/figures/Fig19c_dd_robustness_beta.png")

dir.create(dirname(out_raw), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_fig), recursive = TRUE, showWarnings = FALSE)

year_min  <- 2022L
year_max  <- 2024L
post_year <- 2023L

# Wild bootstrap settings
B_boot <- 999

# -----------------------------
# 3) Sample definition (LOCKED)
# -----------------------------
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

controls <- c("US", "CA")

# -----------------------------
# 4) Load master panel & construct DD-robust outcome
# -----------------------------
print_section("4. Load panel & build DD-robust outcome")

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_count", "cp_count", "dd_count", "is_eu")
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
    (country %in% controls) | (country %in% eu_complete)
  ) |>
  dplyr::mutate(
    EU   = as.integer(country %in% eu_complete),
    Post = as.integer(year >= post_year),

    # Robustness definition (INTENDED):
    # BankShare^(CT+DD) = (CT + DD) / (CT + DD + Card)
    denom = ct_sent_count + dd_count + cp_count,
    bankshare_ct_dd = dplyr::if_else(
      !is.na(denom) & denom > 0,
      (ct_sent_count + dd_count) / denom,
      NA_real_
    )
  ) |>
  dplyr::select(country, year, EU, Post, bankshare_ct_dd) |>
  dplyr::arrange(country, year)

# Guardrails
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

cat("Sample summary:\n")
cat("Years: ", year_min, "–", year_max, "\n", sep = "")
cat("Post definition: Post=1 if year >= ", post_year, "\n", sep = "")
cat("Rows: ", nrow(df), "\n", sep = "")
cat("Countries: ", dplyr::n_distinct(df$country), "\n\n", sep = "")

# -----------------------------
# 5) CRITICAL FIX: Pre-drop NA rows (avoid boottest length mismatch)
# -----------------------------
print_section("5. Pre-drop NA (critical for boottest stability)")

df_est <- df |>
  dplyr::filter(!is.na(bankshare_ct_dd)) |>
  dplyr::filter(!is.na(EU), !is.na(Post)) |>
  dplyr::filter(!is.na(country), !is.na(year)) |>
  dplyr::mutate(
    country = as.factor(country),
    year    = as.factor(year)
  )

cat("Rows after dropping NA LHS: ", nrow(df_est), "\n", sep = "")
cat("Countries after dropping NA LHS: ", dplyr::n_distinct(df_est$country), "\n\n", sep = "")

if (nrow(df_est) == 0) stop("No rows left after dropping NA outcomes. Check dd_count / counts availability.")
if (dplyr::n_distinct(df_est$country) < 5) {
  cat("WARNING: Very few clusters after NA filtering. Bootstrap inference may be unstable.\n\n")
}

# -----------------------------
# 6) Estimate TWFE DiD
# -----------------------------
print_section("6. Estimate TWFE DiD")

# Model: y = alpha_c + lambda_t + beta*(EU×Post) + e
m <- fixest::feols(
  bankshare_ct_dd ~ EU:Post | country + year,
  data = df_est,
  vcov = ~ country
)

# Guardrails: ensure sample alignment is exact (prevents length mismatch errors)
stopifnot(stats::nobs(m) == nrow(df_est))

term <- "EU:Post"
b <- stats::coef(m)[term]
V <- stats::vcov(m)
se <- sqrt(V[term, term])
t_stat <- b / se

# NOTE: this "cluster p" is a normal-approx p-value for the clustered t-stat.
# Your primary benchmark remains the wild cluster bootstrap p-value below.
p_clust <- 2 * stats::pnorm(abs(t_stat), lower.tail = FALSE)

ci_lo <- b - 1.96 * se
ci_hi <- b + 1.96 * se

# -----------------------------
# 7) Wild cluster bootstrap p-value
# -----------------------------
print_section("7. Wild cluster bootstrap p-value")

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

bt <- fwildclusterboot::boottest(
  m,
  clustid = "country",
  param   = term,
  B       = B_boot,
  type    = "webb"
)

p_wild <- NA_real_
if (!is.null(bt$p_val)) {
  p_wild <- as.numeric(bt$p_val)
} else if (!is.null(bt$p.value)) {
  p_wild <- as.numeric(bt$p.value)
} else {
  stop("Could not find p-value in boottest() result. Inspect object `bt`.")
}

# -----------------------------
# 8) Save outputs (CSV + LaTeX + figure)
# -----------------------------
print_section("8. Save outputs")

results <- tibble::tibble(
  outcome         = "BankShare_CTplusDD",
  term            = term,
  beta            = as.numeric(b),
  se_cluster      = as.numeric(se),
  t_cluster       = as.numeric(t_stat),
  p_cluster       = as.numeric(p_clust),
  p_wild          = as.numeric(p_wild),
  ci95_lo         = as.numeric(ci_lo),
  ci95_hi         = as.numeric(ci_hi),
  n_obs           = as.integer(stats::nobs(m)),
  n_countries     = as.integer(dplyr::n_distinct(df_est$country)),
  years           = paste0(year_min, "–", year_max),
  post_definition = paste0("Post=1 if year≥", post_year),
  clusters        = "country",
  fe_country      = "Yes",
  fe_year         = "Yes"
)

readr::write_csv(results, out_raw)

# LaTeX fragment (simple, 1-column table body)
fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)
}

latex_lines <- c(
  "% Auto-generated by scripts/19c_dd_robustness.R",
  "% One-spec robustness: bank-based = CT+DD; years 2022–2024",
  paste0("EU $\\times$ Post(2023+) & \\textbf{", fmt_num(results$beta), "} \\\\"),
  paste0("& (", fmt_num(results$se_cluster), ") \\\\"),
  paste0("Wild bootstrap p-value & ", fmt_p(results$p_wild), " \\\\"),
  "\\midrule",
  paste0("Observations & ", results$n_obs, " \\\\"),
  paste0("Countries & ", results$n_countries, " \\\\"),
  paste0("Years & ", year_min, "--", year_max, " \\\\")
)
writeLines(latex_lines, out_tex)

# Simple coefficient plot (single point + CI)
plot_df <- tibble::tibble(
  outcome = "BankShare^(CT+DD)",
  beta    = results$beta,
  ci95_lo = results$ci95_lo,
  ci95_hi = results$ci95_hi
)

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = outcome, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::labs(
    title = "DD robustness: DiD estimate (EU × Post) including Direct Debits",
    subtitle = paste0(
      "Outcome: (CT + DD) / (CT + DD + Card). Years ", year_min, "–", year_max,
      ". Clustered SE 95% CI. Wild bootstrap p-value in table. (B=", B_boot, ")"
    ),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12)

ggplot2::ggsave(out_fig, p, width = 7.5, height = 4.5, dpi = 300)

cat("Saved outputs:\n")
cat(" - Raw results:   ", out_raw, "\n", sep = "")
cat(" - LaTeX fragment:", out_tex, "\n", sep = "")
cat(" - Figure:        ", out_fig, "\n\n", sep = "")

cat("Result summary:\n")
print(results |> dplyr::select(outcome, beta, se_cluster, p_wild, n_obs, n_countries))

cat("\n19c_dd_robustness.R complete.\n")
