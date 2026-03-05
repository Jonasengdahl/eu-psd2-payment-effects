# scripts/17d_baseline_did_controls_with_CH.R
# ============================================================
# Baseline DiD + controls (counts only; CT vs Card; GDPpc + broadband)
# ROBUSTNESS PATH: includes CH in control group (US + CA + CH)
#
# Key fix vs failing version:
#   - For EACH spec, we explicitly build a NA-clean estimation dataset
#     BEFORE calling feols(), so boottest() never sees mismatched
#     cluster vectors vs kept observations.
#
# Outputs:
#   - outputs/tables/tab17d_baseline_did_controls_raw_with_CH.csv
#   - outputs/tables/tab17d_baseline_did_controls_latex_with_CH.tex
#   - outputs/figures/Fig17d_beta_stability_controls_with_CH.png
# ============================================================

source(here::here("scripts/00_setup.R"))

# ---- 1) Packages --------------------------------------------------------

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install it with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install it with install.packages('fwildclusterboot').")
}

# ---- 2) Paths & settings ------------------------------------------------

in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year_with_CH.rds")

out_raw <- here::here("outputs/tables/tab17d_baseline_did_controls_raw_with_CH.csv")
out_tex <- here::here("outputs/tables/tab17d_baseline_did_controls_latex_with_CH.tex")
out_fig <- here::here("outputs/figures/Fig17d_beta_stability_controls_with_CH.png")

year_min  <- 2012L
year_max  <- 2023L
post_year <- 2018L

B_boot <- 999

# ---- 3) Sample definition (LOCKED EU list; controls expanded) ----------

eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)

controls <- c("US", "CA", "CH")

# ---- 4) Load master panel & construct base dataframe --------------------

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_count", "cp_count", "log_gdppc", "log1p_broadband")
stopifnot(all(req_cols %in% names(panel)))

df0 <- panel %>%
  dplyr::filter(
    year >= year_min, year <= year_max,
    (country %in% controls) | (country %in% eu_complete)
  ) %>%
  dplyr::mutate(
    EU   = as.integer(country %in% eu_complete),
    Post = as.integer(year >= post_year),

    bankshare_ct     = ct_sent_count / (ct_sent_count + cp_count),
    bankcardratio_ct = dplyr::if_else(cp_count > 0, ct_sent_count / cp_count, NA_real_),

    # Dedicated cluster var
    cluster_id = country
  ) %>%
  dplyr::select(
    country, year, cluster_id, EU, Post,
    bankshare_ct, bankcardratio_ct,
    log_gdppc, log1p_broadband
  ) %>%
  dplyr::arrange(country, year)

# Guardrails
stopifnot(all(unique(df0$country[df0$EU == 0]) %in% controls))
stopifnot(all(unique(df0$country[df0$EU == 1]) %in% eu_complete))

# ---- 5) Estimation function (CRITICAL: NA-clean dataset per spec) -------

build_estimation_df <- function(data, yvar, controls) {
  keep_vars <- c("country", "year", "cluster_id", "EU", "Post", yvar, controls)
  x <- data %>%
    dplyr::select(dplyr::all_of(keep_vars))

  # Explicit NA omission on exactly the variables used in THIS spec
  x <- stats::na.omit(x)

  # Ensure factor types (helps keep cluster handling stable)
  x$country <- as.factor(x$country)
  x$year <- as.factor(x$year)
  x$cluster_id <- droplevels(as.factor(x$cluster_id))

  x
}

estimate_did <- function(data, yvar, controls = character(0)) {

  est_df <- build_estimation_df(data, yvar, controls)

  rhs <- "EU:Post"
  if (length(controls) > 0) {
    rhs <- paste(rhs, paste(controls, collapse = " + "), sep = " + ")
  }

  fml <- stats::as.formula(paste0(yvar, " ~ ", rhs))

  model <- fixest::feols(
    fml   = fml,
    data  = est_df,
    fixef = c("country", "year"),
    vcov  = ~ cluster_id
  )

  # Store n_obs explicitly (sometimes helpful for debugging)
  attr(model, "n_obs_used") <- nrow(est_df)

  model
}

# ---- 6) Run models (2 outcomes x 3 specs) -------------------------------

specs <- list(
  S1_baseline        = character(0),
  S2_plus_gdppc      = c("log_gdppc"),
  S3_plus_gdppc_bb   = c("log_gdppc", "log1p_broadband")
)

models <- list(
  BankShare_ct = lapply(specs, function(x) estimate_did(df0, "bankshare_ct", x)),
  BankCardRatio_ct = lapply(specs, function(x) estimate_did(df0, "bankcardratio_ct", x))
)

# n_countries differs by spec if NA-dropping removes some country-years,
# but the number of unique countries should remain the same here.
# We report the TOTAL country count in the (intended) sample frame:
n_countries_total <- dplyr::n_distinct(df0$country)

# ---- 7) Extract clustered results --------------------------------------

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
    n_obs          = nobs(model),
    n_countries    = as.integer(n_countries_total)
  )
}

controls_labels <- c(
  S1_baseline      = "No",
  S2_plus_gdppc    = "log GDPpc",
  S3_plus_gdppc_bb = "log GDPpc + log(1+broadband)"
)

results <- dplyr::bind_rows(
  extract_main(models$BankShare_ct$S1_baseline,          "BankShare_ct",     "1", controls_labels["S1_baseline"],       n_countries_total),
  extract_main(models$BankShare_ct$S2_plus_gdppc,        "BankShare_ct",     "2", controls_labels["S2_plus_gdppc"],     n_countries_total),
  extract_main(models$BankShare_ct$S3_plus_gdppc_bb,     "BankShare_ct",     "3", controls_labels["S3_plus_gdppc_bb"],  n_countries_total),

  extract_main(models$BankCardRatio_ct$S1_baseline,      "BankCardRatio_ct", "1", controls_labels["S1_baseline"],       n_countries_total),
  extract_main(models$BankCardRatio_ct$S2_plus_gdppc,    "BankCardRatio_ct", "2", controls_labels["S2_plus_gdppc"],     n_countries_total),
  extract_main(models$BankCardRatio_ct$S3_plus_gdppc_bb, "BankCardRatio_ct", "3", controls_labels["S3_plus_gdppc_bb"],  n_countries_total)
) %>%
  dplyr::mutate(
    years = paste0(year_min, "–", year_max),
    post_definition = paste0("Post=1 if year≥", post_year),
    clusters = "country",
    fe_country = "Yes",
    fe_year = "Yes"
  )

# ---- 8) Wild cluster bootstrap p-values --------------------------------

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

wild_pval <- function(model) {
  # IMPORTANT: clustid uses cluster_id that is inside the NA-clean est_df
  bt <- fwildclusterboot::boottest(
    model,
    clustid = "cluster_id",
    param   = "EU:Post",
    B       = B_boot,
    type    = "webb"
  )
  if (!is.null(bt$p_val))   return(as.numeric(bt$p_val))
  if (!is.null(bt$p.value)) return(as.numeric(bt$p.value))
  stop("Could not find p-value in boottest() result.")
}

pvals <- c(
  wild_pval(models$BankShare_ct$S1_baseline),
  wild_pval(models$BankShare_ct$S2_plus_gdppc),
  wild_pval(models$BankShare_ct$S3_plus_gdppc_bb),
  wild_pval(models$BankCardRatio_ct$S1_baseline),
  wild_pval(models$BankCardRatio_ct$S2_plus_gdppc),
  wild_pval(models$BankCardRatio_ct$S3_plus_gdppc_bb)
)

results <- results %>% dplyr::mutate(p_wild = pvals)

# Save raw results
readr::write_csv(results, out_raw)

# ---- 9) LaTeX fragment (Panel A only) ----------------------------------

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) ifelse(is.na(p), "", ifelse(p < 0.001, "<0.001", formatC(p, format = "f", digits = 3)))

cell <- function(outcome, spec, x) {
  fmt_num(results %>% dplyr::filter(.data$outcome == outcome, .data$spec == spec) %>% dplyr::pull({{x}}))
}
cell_p <- function(outcome, spec) {
  fmt_p(results %>% dplyr::filter(.data$outcome == outcome, .data$spec == spec) %>% dplyr::pull(p_wild))
}

b_bs  <- c(cell("BankShare_ct","1",beta),        cell("BankShare_ct","2",beta),        cell("BankShare_ct","3",beta))
se_bs <- c(cell("BankShare_ct","1",se_cluster),  cell("BankShare_ct","2",se_cluster),  cell("BankShare_ct","3",se_cluster))
p_bs  <- c(cell_p("BankShare_ct","1"),           cell_p("BankShare_ct","2"),           cell_p("BankShare_ct","3"))

b_br  <- c(cell("BankCardRatio_ct","1",beta),       cell("BankCardRatio_ct","2",beta),       cell("BankCardRatio_ct","3",beta))
se_br <- c(cell("BankCardRatio_ct","1",se_cluster), cell("BankCardRatio_ct","2",se_cluster), cell("BankCardRatio_ct","3",se_cluster))
p_br  <- c(cell_p("BankCardRatio_ct","1"),          cell_p("BankCardRatio_ct","2"),          cell_p("BankCardRatio_ct","3"))

latex_lines <- c(
  "% Auto-generated by scripts/17d_baseline_did_controls_with_CH.R",
  "% ROBUSTNESS PATH: includes CH",
  "% Panel A: Treatment effect + inference (controls robustness)",
  "% BankShare_ct (cols 1-3) then BankCardRatio_ct (cols 4-6)",
  paste0("EU $\\times$ Post(2018+) & \\textbf{", paste(b_bs, collapse = "} & \\textbf{"), "} & \\textbf{", paste(b_br, collapse = "} & \\textbf{"), "} \\\\"),
  paste0("& (", paste(se_bs, collapse = ") & ("), ") & (", paste(se_br, collapse = ") & ("), ") \\\\"),
  paste0("Wild bootstrap p-value & ", paste(p_bs, collapse = " & "), " & ", paste(p_br, collapse = " & "), " \\\\")
)

writeLines(latex_lines, out_tex)

# ---- 10) Optional coefficient stability plot ---------------------------

plot_df <- results %>%
  dplyr::mutate(
    outcome_label = dplyr::case_when(
      outcome == "BankShare_ct" ~ "BankShare",
      outcome == "BankCardRatio_ct" ~ "BankCardRatio",
      TRUE ~ outcome
    ),
    spec_label = dplyr::case_when(
      spec == "1" ~ "No controls",
      spec == "2" ~ "+ log GDPpc",
      spec == "3" ~ "+ log GDPpc + log(1+broadband)",
      TRUE ~ spec
    )
  )

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = spec_label, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::facet_wrap(~ outcome_label, scales = "free_y") +
  ggplot2::labs(
    title = "Stability of DiD estimate (EU × Post) with controls — Robustness incl. CH",
    subtitle = paste0("Clustered SE 95% CI; wild bootstrap p-values reported in table. B=", B_boot),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12) +
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 15, hjust = 1))

ggplot2::ggsave(out_fig, p, width = 10, height = 5.5, dpi = 300)

# ---- 11) Console summary ------------------------------------------------

message("17d_baseline_did_controls_with_CH.R complete.")
message("Saved outputs:")
message(" - Raw results:   ", out_raw)
message(" - LaTeX fragment:", out_tex)
message(" - Stability plot:", out_fig)

print(results %>% dplyr::select(outcome, spec, controls_label, beta, se_cluster, p_wild, n_obs, n_countries))

