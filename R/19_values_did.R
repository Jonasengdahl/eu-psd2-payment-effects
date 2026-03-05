# scripts/19_values_did.R
# ============================================================
# Measurement robustness (VALUES): Baseline DiD (no controls)
#
# Mirrors Stage 2 (counts), but constructs outcomes using values:
#   (1) BankShare_ct_value     = CT_value / (CT_value + Card_value)
#   (2) BankCardRatio_ct_value = CT_value / Card_value
#
# Sample (LOCKED):
#   Years: 2012–2023
#   EU: complete baseline coverage countries (same list as Stage 2/3)
#   Controls: US + Canada
#
# Inference:
#   Clustered SEs at country level + wild cluster bootstrap p-values
#
# Outputs:
#   - outputs/tables/tab19_values_did_raw.csv
#   - outputs/tables/tab19_values_did_latex.tex
#   - outputs/figures/Fig19_values_did_beta.png
# ============================================================

source(here::here("scripts/00_setup.R"))

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install with install.packages('fwildclusterboot').")
}
if (!requireNamespace("tibble", quietly = TRUE)) {
  stop("Package 'tibble' is required. Install with install.packages('tibble').")
}

# ---- Paths & settings ---------------------------------------------------

in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

out_raw <- here::here("outputs/tables/tab19_values_did_raw.csv")
out_tex <- here::here("outputs/tables/tab19_values_did_latex.tex")
out_fig <- here::here("outputs/figures/Fig19_values_did_beta.png")

year_min  <- 2012L
year_max  <- 2023L
post_year <- 2018L

B_boot <- 999

# ---- Sample definition (LOCKED) ----------------------------------------

eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

controls <- c("US", "CA")

# ---- Load master panel & build VALUE outcomes ---------------------------

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_value", "cp_value")
stopifnot(all(req_cols %in% names(panel)))

df <- panel %>%
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) %>%
  dplyr::mutate(
    EU   = as.integer(country %in% eu_complete),
    Post = as.integer(year >= post_year),
    bankshare_ct_value = ct_sent_value / (ct_sent_value + cp_value),
    bankcardratio_ct_value = dplyr::if_else(cp_value > 0, ct_sent_value / cp_value, NA_real_)
  ) %>%
  dplyr::select(country, year, EU, Post, bankshare_ct_value, bankcardratio_ct_value) %>%
  dplyr::arrange(country, year)

# Guardrails
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

n_countries_total <- dplyr::n_distinct(df$country)

# ---- Estimation helpers -------------------------------------------------

estimate_did <- function(data, yvar) {
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

extract_main <- function(model, outcome_name, n_countries_total) {
  term <- "EU:Post"
  b  <- stats::coef(model)[term]
  V  <- stats::vcov(model)
  se <- sqrt(V[term, term])

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
    n_obs       = stats::nobs(model),
    n_countries = as.integer(n_countries_total)
  )
}

wild_pval <- function(model) {
  set.seed(123)
  if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

  bt <- fwildclusterboot::boottest(
    model,
    clustid = "country",
    param   = "EU:Post",
    B       = B_boot,
    type    = "webb"
  )

  if (!is.null(bt$p_val)) return(as.numeric(bt$p_val))
  if (!is.null(bt$p.value)) return(as.numeric(bt$p.value))
  stop("Could not find p-value in boottest() result. Inspect object `bt`.")
}

# ---- Run models ---------------------------------------------------------

m1 <- estimate_did(df, "bankshare_ct_value")
m2 <- estimate_did(df, "bankcardratio_ct_value")

res1 <- extract_main(m1, "BankShare_ct_value", n_countries_total)
res2 <- extract_main(m2, "BankCardRatio_ct_value", n_countries_total)

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
    controls        = "No"
  )

readr::write_csv(results, out_raw)

# ---- LaTeX fragment -----------------------------------------------------

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) if (is.na(p)) "" else if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)

b1 <- fmt_num(results$beta[results$outcome == "BankShare_ct_value"])
s1 <- fmt_num(results$se_cluster[results$outcome == "BankShare_ct_value"])
p1 <- fmt_p(results$p_wild[results$outcome == "BankShare_ct_value"])
n1 <- results$n_obs[results$outcome == "BankShare_ct_value"]
c1 <- results$n_countries[results$outcome == "BankShare_ct_value"]

b2 <- fmt_num(results$beta[results$outcome == "BankCardRatio_ct_value"])
s2 <- fmt_num(results$se_cluster[results$outcome == "BankCardRatio_ct_value"])
p2 <- fmt_p(results$p_wild[results$outcome == "BankCardRatio_ct_value"])
n2 <- results$n_obs[results$outcome == "BankCardRatio_ct_value"]
c2 <- results$n_countries[results$outcome == "BankCardRatio_ct_value"]

latex_lines <- c(
  "% Auto-generated by scripts/19_values_did.R",
  paste0("EU $\\times$ Post(2018+) & \\textbf{", b1, "} & \\textbf{", b2, "} \\\\"),
  paste0("& (", s1, ") & (", s2, ") \\\\"),
  paste0("Wild bootstrap p-value & ", p1, " & ", p2, " \\\\"),
  "\\midrule",
  paste0("Observations & ", n1, " & ", n2, " \\\\"),
  paste0("Countries & ", c1, " & ", c2, " \\\\"),
  paste0("Years & ", year_min, "--", year_max, " & ", year_min, "--", year_max, " \\\\")
)

writeLines(latex_lines, out_tex)

# ---- Coefficient plot ---------------------------------------------------

plot_df <- results %>%
  dplyr::mutate(
    outcome_label = dplyr::case_when(
      outcome == "BankShare_ct_value" ~ "BankShare (values)",
      outcome == "BankCardRatio_ct_value" ~ "BankCardRatio (values)",
      TRUE ~ outcome
    )
  )

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = outcome_label, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::labs(
    title = "DiD estimates (EU × Post) — values-based outcomes",
    subtitle = paste0("Clustered SEs with 95% CI; wild bootstrap p-values in table. (B = ", B_boot, ")"),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12)

ggplot2::ggsave(out_fig, p, width = 8, height = 5, dpi = 300)

message("19_values_did.R complete.")
message("Saved outputs:")
message(" - Raw results:   ", out_raw)
message(" - LaTeX fragment:", out_tex)
message(" - Coef plot:     ", out_fig)

print(results %>% dplyr::select(outcome, beta, se_cluster, p_wild, n_obs, n_countries))
