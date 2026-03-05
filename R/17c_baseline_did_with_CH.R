# scripts/17c_baseline_did_with_CH.R
# ============================================================
# Baseline DiD estimation (counts only; CT vs Card; no controls)
# ROBUSTNESS PATH: includes CH in control group (US + CA + CH)
#
# Purpose (Stage 2 robustness):
#   - Same TWFE DiD as baseline Table 1, but expand controls to include CH
#   - Outcomes (counts only):
#       (1) BankShare_ct      = CT / (CT + Card)
#       (2) BankCardRatio_ct  = CT / Card
#   - Sample:
#       Years: 2012–2023
#       EU: complete baseline coverage countries (locked list)
#       Controls: US + Canada + Switzerland (CH)
#   - Inference:
#       Clustered SEs at country level + wild cluster bootstrap p-values
#
# Outputs (ROBUSTNESS PATH):
#   - outputs/tables/tab17c_baseline_did_counts_raw_with_CH.csv
#   - outputs/tables/tab17c_baseline_did_counts_latex_with_CH.tex  (LaTeX fragment)
#   - outputs/figures/Fig17c_baseline_did_beta_with_CH.png         (optional coef plot)
# ============================================================

source(here::here("scripts/00_setup.R"))

# ---- 1) Packages required for estimation/inference ----------------------

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install it with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install it with install.packages('fwildclusterboot').")
}

# ---- 2) Paths & settings ------------------------------------------------

# ROBUSTNESS PATH: includes CH
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year_with_CH.rds")

out_raw <- here::here("outputs/tables/tab17c_baseline_did_counts_raw_with_CH.csv")
out_tex <- here::here("outputs/tables/tab17c_baseline_did_counts_latex_with_CH.tex")
out_fig <- here::here("outputs/figures/Fig17c_baseline_did_beta_with_CH.png")

year_min  <- 2012L
year_max  <- 2023L
post_year <- 2018L

# Wild bootstrap settings
B_boot <- 999  # 999 is usually fine for a thesis; increase later if desired

# ---- 3) Sample definition (LOCKED EU list; controls extended) -----------

eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

# ROBUSTNESS PATH: includes CH
controls <- c("US", "CA", "CH")

# ---- 4) Load master panel & construct baseline outcomes -----------------

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "is_eu", "ct_sent_count", "cp_count")
stopifnot(all(req_cols %in% names(panel)))

df <- panel %>%
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) %>%
  dplyr::mutate(
    EU   = as.integer(country %in% eu_complete),
    Post = as.integer(year >= post_year),
    bankshare_ct     = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = dplyr::if_else(cp_count > 0, ct_sent_count / cp_count, NA_real_)
  ) %>%
  dplyr::select(country, year, EU, Post, bankshare_ct, bankcardratio_ct) %>%
  dplyr::arrange(country, year)

# Basic guardrails
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))  # EU==0 should be controls only
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

# Pre-compute country count robustly (avoid relying on fixest internal slots)
n_countries_total <- dplyr::n_distinct(df$country)

# ---- 5) Estimate TWFE DiD models ---------------------------------------
# Model: y_ct = alpha_c + lambda_t + beta*(EU_c x Post_t) + eps_ct

estimate_did <- function(data, yvar) {

  # Make fixed effects unambiguous
  data$country <- as.factor(data$country)
  data$year    <- as.factor(data$year)

  fml <- stats::reformulate("EU:Post", response = yvar)

  fixest::feols(
    fml   = fml,
    data  = data,
    fixef = c("country", "year"),
    vcov  = ~ country
  )
}

m1 <- estimate_did(df, "bankshare_ct")
m2 <- estimate_did(df, "bankcardratio_ct")

# ---- 6) Extract clustered-SE results -----------------------------------

extract_main <- function(model, outcome_name, n_countries_total) {
  term <- "EU:Post"

  b  <- coef(model)[term]
  V  <- vcov(model)
  se <- sqrt(V[term, term])

  # clustered p-value (normal approx; reference only)
  t_stat  <- b / se
  p_clust <- 2 * stats::pnorm(abs(t_stat), lower.tail = FALSE)

  ci_lo <- b - 1.96 * se
  ci_hi <- b + 1.96 * se

  tibble::tibble(
    outcome     = outcome_name,
    term        = term,
    beta        = as.numeric(b),
    se_cluster  = as.numeric(se),
    t_cluster   = as.numeric(t_stat),
    p_cluster   = as.numeric(p_clust),
    ci95_lo     = as.numeric(ci_lo),
    ci95_hi     = as.numeric(ci_hi),
    n_obs       = nobs(model),
    n_countries = as.integer(n_countries_total)
  )
}

res1 <- extract_main(m1, "BankShare_ct", n_countries_total)
res2 <- extract_main(m2, "BankCardRatio_ct", n_countries_total)

# ---- 7) Wild cluster bootstrap p-values ---------------------------------
# Bootstrap H0: EU:Post = 0, clustered by country (few clusters).

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

wild_pval <- function(model) {

  bt <- fwildclusterboot::boottest(
    model,
    clustid = "country",
    param   = "EU:Post",
    B       = B_boot,
    type    = "webb"   # if your build errors here, change to "rademacher"
  )

  if (!is.null(bt$p_val))   return(as.numeric(bt$p_val))
  if (!is.null(bt$p.value)) return(as.numeric(bt$p.value))

  stop("Could not find p-value in boottest() result. Inspect object `bt`.")
}

p_wild_1 <- wild_pval(m1)
p_wild_2 <- wild_pval(m2)

results <- dplyr::bind_rows(res1, res2) %>%
  dplyr::mutate(
    p_wild          = c(p_wild_1, p_wild_2),
    years           = paste0(year_min, "–", year_max),
    post_definition = paste0("Post=1 if year≥", post_year),
    clusters        = "country",
    fe_country      = "Yes",
    fe_year         = "Yes",
    controls        = "No",
    robustness      = "Controls include CH"
  )

readr::write_csv(results, out_raw)

# ---- 8) Create LaTeX fragment ------------------------------------------

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)
}

b1 <- fmt_num(results$beta[results$outcome == "BankShare_ct"])
s1 <- fmt_num(results$se_cluster[results$outcome == "BankShare_ct"])
p1 <- fmt_p(results$p_wild[results$outcome == "BankShare_ct"])
n1 <- results$n_obs[results$outcome == "BankShare_ct"]
c1 <- results$n_countries[results$outcome == "BankShare_ct"]

b2 <- fmt_num(results$beta[results$outcome == "BankCardRatio_ct"])
s2 <- fmt_num(results$se_cluster[results$outcome == "BankCardRatio_ct"])
p2 <- fmt_p(results$p_wild[results$outcome == "BankCardRatio_ct"])
n2 <- results$n_obs[results$outcome == "BankCardRatio_ct"]
c2 <- results$n_countries[results$outcome == "BankCardRatio_ct"]

latex_lines <- c(
  "% Auto-generated by scripts/17c_baseline_did_with_CH.R (ROBUSTNESS PATH: includes CH)",
  "% Panel: Treatment effect + inference",
  paste0("EU $\\times$ Post(2018+) & \\textbf{", b1, "} & \\textbf{", b2, "} \\\\"),
  paste0("& (", s1, ") & (", s2, ") \\\\"),
  paste0("Wild bootstrap p-value & ", p1, " & ", p2, " \\\\"),
  "\\midrule",
  paste0("Observations & ", n1, " & ", n2, " \\\\"),
  paste0("Countries & ", c1, " & ", c2, " \\\\"),
  paste0("Years & ", year_min, "--", year_max, " & ", year_min, "--", year_max, " \\\\")
)

writeLines(latex_lines, out_tex)

# ---- 9) Optional coefficient plot --------------------------------------

plot_df <- results %>%
  dplyr::mutate(
    outcome_label = dplyr::case_when(
      outcome == "BankShare_ct" ~ "BankShare (counts)",
      outcome == "BankCardRatio_ct" ~ "BankCardRatio (counts)",
      TRUE ~ outcome
    )
  )

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = outcome_label, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::labs(
    title = "Baseline DiD estimates (EU × Post) — Robustness incl. CH",
    subtitle = paste0(
      "Counts-based outcomes; clustered SEs with 95% CI. ",
      "Wild bootstrap p-values in table. (B = ", B_boot, ")"
    ),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12)

ggplot2::ggsave(out_fig, p, width = 8, height = 5, dpi = 300)

# ---- 10) Console summary ------------------------------------------------

message("17c_baseline_did_with_CH.R complete.")
message("Saved outputs:")
message(" - Raw results:   ", out_raw)
message(" - LaTeX fragment:", out_tex)
message(" - Coef plot:     ", out_fig)

message("Key coefficient (EU x Post) with wild bootstrap p-values:")
print(results %>% dplyr::select(outcome, beta, se_cluster, p_wild, n_obs, n_countries))
