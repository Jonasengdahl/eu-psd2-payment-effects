# scripts/19b_values_did_controls.R
# ============================================================
# Measurement robustness (VALUES): Baseline DiD + controls
#
# Sequential specs:
#   (1) No controls
#   (2) + log(GDPpc)
#   (3) + log(GDPpc) + log(1 + broadband)
#
# Uses Stage 3 sample restriction (EU locked list excludes DK/LU/MT),
# and the MASTER PANEL WITH CONTROLS created in script 15.
#
# Outputs:
#   - outputs/tables/tab19b_values_did_controls_raw.csv
#   - outputs/tables/tab19b_values_did_controls_latex.tex
#   - outputs/figures/Fig19b_values_beta_stability_controls.png
#   - outputs/logs/19b_values_did_controls.log
# ============================================================

source(here::here("scripts/00_setup.R"))

# ---- 0) Logging ---------------------------------------------------------
log_file <- here::here("outputs/logs/19b_values_did_controls.log")
sink(log_file, split = TRUE)
on.exit({
  try(sink(), silent = TRUE)
}, add = TRUE)

cat("=========================================\n")
cat("19b_values_did_controls.R — START\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("=========================================\n\n")

# ---- 1) Packages --------------------------------------------------------
cat("=========================================\n")
cat("1. Package checks\n")
cat("=========================================\n\n")

if (!requireNamespace("fixest", quietly = TRUE)) {
  stop("Package 'fixest' is required. Install with install.packages('fixest').")
}
if (!requireNamespace("fwildclusterboot", quietly = TRUE)) {
  stop("Package 'fwildclusterboot' is required. Install with install.packages('fwildclusterboot').")
}
if (!requireNamespace("tibble", quietly = TRUE)) {
  stop("Package 'tibble' is required. Install with install.packages('tibble').")
}

# ---- 2) Paths & settings ------------------------------------------------
cat("=========================================\n")
cat("2. Paths & settings\n")
cat("=========================================\n\n")

# IMPORTANT: use master panel WITH controls (script 15 output)
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_controls_country_year.rds")

out_raw <- here::here("outputs/tables/tab19b_values_did_controls_raw.csv")
out_tex <- here::here("outputs/tables/tab19b_values_did_controls_latex.tex")
out_fig <- here::here("outputs/figures/Fig19b_values_beta_stability_controls.png")

year_min  <- 2012L
year_max  <- 2023L
post_year <- 2018L

B_boot <- 999

cat("Input panel:\n - ", in_panel_rds, "\n\n", sep = "")
cat("Outputs:\n")
cat(" - ", out_raw, "\n", sep = "")
cat(" - ", out_tex, "\n", sep = "")
cat(" - ", out_fig, "\n\n", sep = "")

cat("Window: years ", year_min, "–", year_max, " | Post=1 if year≥", post_year, " | B=", B_boot, "\n\n", sep = "")

# ---- 3) Sample definition (LOCKED) -------------------------------------
cat("=========================================\n")
cat("3. Sample definition (LOCKED)\n")
cat("=========================================\n\n")

# EU locked list (Stage 3): excludes DK/LU/MT intentionally
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)
controls <- c("US", "CA")

cat("EU treated countries (locked): ", paste(eu_complete, collapse = ", "), "\n", sep = "")
cat("Controls (locked): ", paste(controls, collapse = ", "), "\n", sep = "")
cat("NOTE: DK/LU/MT are intentionally excluded to keep controlled specs on a consistent EU set.\n\n")

# ---- 4) Load panel & construct outcomes --------------------------------
cat("=========================================\n")
cat("4. Load panel & construct outcomes\n")
cat("=========================================\n\n")

stopifnot(file.exists(in_panel_rds))
panel <- readRDS(in_panel_rds)

req_cols <- c("country", "year", "ct_sent_value", "cp_value", "log_gdppc", "log1p_broadband")
stopifnot(all(req_cols %in% names(panel)))

df <- panel %>%
  dplyr::mutate(
    country = as.character(country),
    year    = as.integer(year)
  ) %>%
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
  dplyr::select(
    country, year, EU, Post,
    bankshare_ct_value, bankcardratio_ct_value,
    log_gdppc, log1p_broadband
  ) %>%
  dplyr::arrange(country, year)

# Guardrails: EU==0 should only be controls; EU==1 should only be eu_complete
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

cat("Rows in estimation dataframe: ", nrow(df), "\n", sep = "")
cat("Countries in estimation dataframe: ", dplyr::n_distinct(df$country), "\n\n", sep = "")

# Missing controls check (should be zero across included countries)
miss_tbl <- df %>%
  dplyr::group_by(country, EU) %>%
  dplyr::summarise(
    n = dplyr::n(),
    miss_log_gdppc   = mean(is.na(log_gdppc)),
    miss_log1p_bb    = mean(is.na(log1p_broadband)),
    .groups = "drop"
  ) %>%
  dplyr::arrange(dplyr::desc(miss_log_gdppc + miss_log1p_bb), country)

cat("Control missingness (share) by country, 2012–2023:\n")
print(miss_tbl)
cat("\n")

unexpected_missing <- miss_tbl %>%
  dplyr::filter(miss_log_gdppc > 0 | miss_log1p_bb > 0) %>%
  dplyr::pull(country)

if (length(unexpected_missing) > 0) {
  stop(
    "Unexpected missing controls within the locked estimation sample: ",
    paste(unexpected_missing, collapse = ", "),
    "\nThis should not happen if you are using the script 15 controls master."
  )
}

n_countries_total <- dplyr::n_distinct(df$country)

# ---- 5) Estimation helpers ---------------------------------------------
cat("=========================================\n")
cat("5. Estimation function\n")
cat("=========================================\n\n")

estimate_did <- function(data, yvar, controls_vec = character(0)) {
  data$country <- as.factor(data$country)
  data$year    <- as.factor(data$year)

  rhs <- "EU:Post"
  if (length(controls_vec) > 0) {
    rhs <- paste(rhs, paste(controls_vec, collapse = " + "), sep = " + ")
  }

  fml <- stats::as.formula(paste0(yvar, " ~ ", rhs))

  fixest::feols(
    fml   = fml,
    data  = data,
    fixef = c("country", "year"),
    vcov  = ~ country
  )
}

extract_main <- function(model, outcome_name, spec_id, controls_label, n_countries_total) {
  term <- "EU:Post"
  b  <- stats::coef(model)[term]
  V  <- stats::vcov(model)            # IMPORTANT: stats::vcov(), not fixest::vcov()
  se <- sqrt(V[term, term])

  t_stat  <- b / se
  p_clust <- 2 * stats::pnorm(abs(t_stat), lower.tail = FALSE)

  ci_lo <- b - 1.96 * se
  ci_hi <- b + 1.96 * se

  tibble::tibble(
    outcome        = outcome_name,
    spec           = as.character(spec_id),
    controls_label = controls_label,
    term           = term,
    beta           = as.numeric(b),
    se_cluster     = as.numeric(se),
    t_cluster      = as.numeric(t_stat),
    p_cluster      = as.numeric(p_clust),
    ci95_lo        = as.numeric(ci_lo),
    ci95_hi        = as.numeric(ci_hi),
    n_obs          = stats::nobs(model),   # IMPORTANT: stats::nobs()
    n_countries    = as.integer(n_countries_total)
  )
}

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
  stop("Could not find p-value in boottest() result. Inspect object `bt`.")
}

# ---- 6) Estimate models (2 outcomes × 3 specs) --------------------------
cat("=========================================\n")
cat("6. Estimate models (2 outcomes × 3 specs)\n")
cat("=========================================\n\n")

set.seed(123)
if (requireNamespace("dqrng", quietly = TRUE)) dqrng::dqset.seed(123)

specs <- list(
  `1` = list(label = "No controls", controls = character(0)),
  `2` = list(label = "log GDPpc", controls = c("log_gdppc")),
  `3` = list(label = "log GDPpc + log(1+broadband)", controls = c("log_gdppc", "log1p_broadband"))
)

run_specs_for_outcome <- function(yvar, outcome_name) {
  out <- lapply(names(specs), function(sid) {
    sp <- specs[[sid]]
    m  <- estimate_did(df, yvar, sp$controls)
    r  <- extract_main(m, outcome_name, sid, sp$label, n_countries_total)
    r$p_wild <- wild_pval(m)
    r
  })
  dplyr::bind_rows(out)
}

results <- dplyr::bind_rows(
  run_specs_for_outcome("bankshare_ct_value", "BankShare_ct_value"),
  run_specs_for_outcome("bankcardratio_ct_value", "BankCardRatio_ct_value")
) %>%
  dplyr::mutate(
    years           = paste0(year_min, "–", year_max),
    post_definition = paste0("Post=1 if year≥", post_year),
    clusters        = "country",
    fe_country      = "Yes",
    fe_year         = "Yes"
  )

readr::write_csv(results, out_raw)

# ---- 7) LaTeX fragment --------------------------------------------------
cat("=========================================\n")
cat("7. Write LaTeX fragment\n")
cat("=========================================\n\n")

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) if (is.na(p)) "" else if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)

make_row_block <- function(outcome_key) {
  sub <- results %>% dplyr::filter(outcome == outcome_key) %>% dplyr::arrange(as.integer(spec))
  b <- fmt_num(sub$beta)
  s <- fmt_num(sub$se_cluster)
  p <- vapply(sub$p_wild, fmt_p, character(1))

  c(
    paste0("EU $\\times$ Post(2018+) & \\textbf{", b[1], "} & \\textbf{", b[2], "} & \\textbf{", b[3], "} \\\\"),
    paste0("& (", s[1], ") & (", s[2], ") & (", s[3], ") \\\\"),
    paste0("Wild bootstrap p-value & ", p[1], " & ", p[2], " & ", p[3], " \\\\")
  )
}

latex_lines <- c(
  "% Auto-generated by scripts/19b_values_did_controls.R",
  "% Outcome: BankShare_ct_value (values)",
  make_row_block("BankShare_ct_value"),
  "\\midrule",
  "% Outcome: BankCardRatio_ct_value (values)",
  make_row_block("BankCardRatio_ct_value")
)

writeLines(latex_lines, out_tex)

# ---- 8) Stability plot --------------------------------------------------
cat("=========================================\n")
cat("8. Save stability plot\n")
cat("=========================================\n\n")

plot_df <- results %>%
  dplyr::mutate(
    outcome_label = dplyr::case_when(
      outcome == "BankShare_ct_value" ~ "BankShare (values)",
      outcome == "BankCardRatio_ct_value" ~ "BankCardRatio (values)",
      TRUE ~ outcome
    ),
    spec_label = factor(
      controls_label,
      levels = c("No controls", "log GDPpc", "log GDPpc + log(1+broadband)")
    )
  )

p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = spec_label, y = beta)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
  ggplot2::geom_point(size = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = ci95_lo, ymax = ci95_hi), width = 0.15) +
  ggplot2::facet_wrap(~ outcome_label, scales = "free_y") +
  ggplot2::labs(
    title = "Stability of DiD estimate (EU × Post) — values outcomes",
    subtitle = paste0("Clustered SE 95% CI; wild bootstrap p-values in table. B=", B_boot),
    x = NULL,
    y = "Estimated effect (β)"
  ) +
  ggplot2::theme_minimal(base_size = 12) +
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 20, hjust = 1))

ggplot2::ggsave(out_fig, p, width = 10, height = 5.5, dpi = 300)

# ---- 9) Complete --------------------------------------------------------
cat("=========================================\n")
cat("9. Complete\n")
cat("=========================================\n\n")

message("19b_values_did_controls.R complete.")
message("Saved outputs:")
message(" - Raw results:    ", out_raw)
message(" - LaTeX fragment: ", out_tex)
message(" - Stability plot: ", out_fig)

print(results %>% dplyr::select(outcome, spec, controls_label, beta, se_cluster, p_wild, n_obs, n_countries))

cat("\n=========================================\n")
cat("19b_values_did_controls.R — END\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("=========================================\n")

