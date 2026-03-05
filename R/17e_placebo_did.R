# scripts/17e_placebo_did.R
# ============================================================
# Placebo DiD test (fake treatment year 2016)
# Purpose (Stage 5, after 17_baseline_did.R):
#   - Assess whether baseline PSD2 DiD results could be driven by spurious
#     EU vs control trend differences.
#   - Assign a fake treatment year (2016) and estimate EU × PostPlacebo.
#
# Design:
#   - Sample: baseline (24 EU + US/CA), years 2012–2023
#   - PostPlacebo = 1{year >= 2016}
#   - TWFE DiD: y_ct = α_c + λ_t + β_placebo (EU_c × PostPlacebo_t) + ε_ct
#
# Outcomes (counts):
#   - BankShare     = CT / (CT + Card)
#   - BankCardRatio = CT / Card
#
# Inference:
#   - Cluster at country level
#   - Wild cluster bootstrap p-values (B = 999), type = "webb"
#
# Outputs:
#   - outputs/tables/tab17e_placebo_did_latex.tex
#   - outputs/tables/tab17e_placebo_did_raw.csv
#   - outputs/figures/Fig17e_placebo_vs_baseline_beta.png  (optional)
#   - outputs/logs/17e_placebo_did_log.txt
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
log_file <- here::here("outputs/logs", "17e_placebo_did_log.txt")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)

on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n============================================================\n")
cat("17e_placebo_did.R\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("============================================================\n\n")

# -----------------------------
# 1) Packages
# -----------------------------
pkgs_needed <- c("dplyr", "tibble", "readr", "ggplot2", "fixest", "fwildclusterboot")
for (p in pkgs_needed) {
  if (!requireNamespace(p, quietly = TRUE)) {
    stop("Package '", p, "' is required. Install it with install.packages('", p, "').")
  }
}

# -----------------------------
# 2) Paths & settings
# -----------------------------
in_panel_rds <- here::here("data/processed/panel/master_panel_payments_country_year.rds")

out_tex <- here::here("outputs/tables/tab17e_placebo_did_latex.tex")
out_raw <- here::here("outputs/tables/tab17e_placebo_did_raw.csv")
out_fig <- here::here("outputs/figures/Fig17e_placebo_vs_baseline_beta.png")

dir.create(dirname(out_tex), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_fig), recursive = TRUE, showWarnings = FALSE)

year_min <- 2012L
year_max <- 2023L
placebo_year <- 2016L
B_boot <- 999

# Baseline sample (LOCKED)
eu_complete <- c(
  "AT","BE","BG","CY","CZ","DE","EE","ES","FI","FR","GR","HR","HU","IE","IT",
  "LT","LV","NL","PL","PT","RO","SE","SI","SK"
)
controls <- c("US", "CA")

# -----------------------------
# 3) Load panel & construct outcomes
# -----------------------------
print_section("3. Load panel & construct placebo outcomes")

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
    (country %in% controls) | (country %in% eu_complete)
  ) |>
  dplyr::mutate(
    EU = as.integer(country %in% eu_complete),
    PostPlacebo = as.integer(year >= placebo_year),

    # Outcomes (counts)
    denom_share = ct_sent_count + cp_count,
    bankshare_ct = dplyr::if_else(!is.na(denom_share) & denom_share > 0,
                                 ct_sent_count / denom_share,
                                 NA_real_),

    bankcardratio_ct = dplyr::if_else(!is.na(cp_count) & cp_count > 0,
                                     ct_sent_count / cp_count,
                                     NA_real_)
  ) |>
  dplyr::select(country, year, EU, PostPlacebo, bankshare_ct, bankcardratio_ct) |>
  dplyr::arrange(country, year)

# Guardrails
stopifnot(all(unique(df$country[df$EU == 0]) %in% controls))
stopifnot(all(unique(df$country[df$EU == 1]) %in% eu_complete))

cat("Sample summary (pre-NA drop):\n")
cat("Years: ", year_min, "–", year_max, "\n", sep = "")
cat("Placebo Post definition: PostPlacebo=1 if year >= ", placebo_year, "\n", sep = "")
cat("Rows: ", nrow(df), "\n", sep = "")
cat("Countries: ", dplyr::n_distinct(df$country), "\n\n", sep = "")

# -----------------------------
# 4) Estimate helper
# -----------------------------
print_section("4. Estimation helper (TWFE + wild bootstrap)")

run_placebo <- function(data, y_var, term = "EU:PostPlacebo", B_boot = 999) {

  df_est <- data |>
    dplyr::filter(!is.na(.data[[y_var]])) |>
    dplyr::filter(!is.na(EU), !is.na(PostPlacebo)) |>
    dplyr::mutate(
      country = as.factor(country),
      year    = as.factor(year)
    )

  if (nrow(df_est) == 0) stop("No rows left after dropping NA for outcome: ", y_var)

  m <- fixest::feols(
    stats::as.formula(paste0(y_var, " ~ EU:PostPlacebo | country + year")),
    data = df_est,
    vcov = ~ country
  )

  # alignment guardrail
  stopifnot(stats::nobs(m) == nrow(df_est))

  b <- stats::coef(m)[term]
  V <- stats::vcov(m)
  se <- sqrt(V[term, term])
  t_stat <- b / se
  p_clust <- 2 * stats::pnorm(abs(t_stat), lower.tail = FALSE)

  ci_lo <- b - 1.96 * se
  ci_hi <- b + 1.96 * se

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
    stop("Could not find p-value in boottest() result for outcome: ", y_var)
  }

  tibble::tibble(
    outcome      = y_var,
    term         = term,
    beta         = as.numeric(b),
    se_cluster   = as.numeric(se),
    t_cluster    = as.numeric(t_stat),
    p_cluster    = as.numeric(p_clust),
    p_wild       = as.numeric(p_wild),
    ci95_lo      = as.numeric(ci_lo),
    ci95_hi      = as.numeric(ci_hi),
    n_obs        = as.integer(stats::nobs(m)),
    n_countries  = as.integer(dplyr::n_distinct(df_est$country))
  )
}

# -----------------------------
# 5) Run placebo for both outcomes
# -----------------------------
print_section("5. Run placebo DiD (EU × PostPlacebo)")

res_share <- run_placebo(df, "bankshare_ct", B_boot = B_boot)
res_ratio <- run_placebo(df, "bankcardratio_ct", B_boot = B_boot)

results <- dplyr::bind_rows(res_share, res_ratio) |>
  dplyr::mutate(
    placebo_year = placebo_year,
    years = paste0(year_min, "–", year_max),
    post_definition = paste0("PostPlacebo=1 if year≥", placebo_year),
    clusters = "country",
    fe_country = "Yes",
    fe_year = "Yes"
  )

readr::write_csv(results, out_raw)

cat("Results summary:\n")
print(results |> dplyr::select(outcome, beta, se_cluster, p_wild, n_obs, n_countries))

# -----------------------------
# 6) Save LaTeX fragment
# -----------------------------
print_section("6. Save LaTeX table fragment")

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)
}

# Map to display labels
label_outcome <- function(y) {
  if (y == "bankshare_ct") return("BankShare (counts)")
  if (y == "bankcardratio_ct") return("BankCardRatio (counts)")
  y
}

latex_lines <- c(
  "% Auto-generated by scripts/17e_placebo_did.R",
  paste0("% Placebo DiD: fake treatment year = ", placebo_year, "; sample 2012–2023; baseline EU sample + US/CA controls"),
  "\\begin{tabular}{lcc}",
  "\\toprule",
  " & BankShare (counts) & BankCardRatio (counts) \\\\",
  "\\midrule"
)

# Ensure order: share then ratio
res_share2 <- results |> dplyr::filter(outcome == "bankshare_ct")
res_ratio2 <- results |> dplyr::filter(outcome == "bankcardratio_ct")

latex_lines <- c(
  latex_lines,
  paste0("EU $\\times$ PostPlacebo(\\textgeq ", placebo_year, ") & \\textbf{", fmt_num(res_share2$beta), "} & \\textbf{", fmt_num(res_ratio2$beta), "} \\\\"),
  paste0("& (", fmt_num(res_share2$se_cluster), ") & (", fmt_num(res_ratio2$se_cluster), ") \\\\"),
  paste0("Wild bootstrap $p$-value & ", fmt_p(res_share2$p_wild), " & ", fmt_p(res_ratio2$p_wild), " \\\\"),
  "\\midrule",
  paste0("Observations & ", res_share2$n_obs, " & ", res_ratio2$n_obs, " \\\\"),
  paste0("Countries & ", res_share2$n_countries, " & ", res_ratio2$n_countries, " \\\\"),
  paste0("Years & ", year_min, "--", year_max, " & ", year_min, "--", year_max, " \\\\"),
  "\\bottomrule",
  "\\end{tabular}"
)

writeLines(latex_lines, out_tex)

cat("Saved outputs:\n")
cat(" - Raw results:   ", out_raw, "\n", sep = "")
cat(" - LaTeX fragment:", out_tex, "\n", sep = "")

# -----------------------------
# 7) Optional figure: compare baseline β vs placebo β
# -----------------------------
print_section("7. Optional figure: baseline vs placebo (if baseline β available)")

# Try to load baseline betas from prior raw output (if it exists).
# If you don't have a raw baseline CSV, this will skip gracefully.
baseline_raw_candidates <- c(
  here::here("outputs/tables/tab17_baseline_did_counts_raw.csv"),
  here::here("outputs/tables/tab17_baseline_did_counts.csv"),
  here::here("outputs/tables/tab17_baseline_did_raw.csv")
)

baseline_path <- baseline_raw_candidates[file.exists(baseline_raw_candidates)][1]
if (!is.na(baseline_path) && file.exists(baseline_path)) {

  base <- tryCatch(readr::read_csv(baseline_path, show_col_types = FALSE), error = function(e) NULL)

  if (!is.null(base) && all(c("outcome", "beta") %in% names(base))) {

    # Attempt to map baseline outcomes to our labels
    base2 <- base |>
      dplyr::mutate(
        outcome_clean = dplyr::case_when(
          grepl("BankShare", outcome, ignore.case = TRUE) ~ "BankShare (counts)",
          grepl("BankCardRatio", outcome, ignore.case = TRUE) ~ "BankCardRatio (counts)",
          TRUE ~ outcome
        )
      ) |>
      dplyr::group_by(outcome_clean) |>
      dplyr::summarise(beta = dplyr::first(beta), .groups = "drop")

    plot_df <- tibble::tibble(
      outcome = c("BankShare (counts)", "BankCardRatio (counts)"),
      placebo_beta = c(res_share2$beta, res_ratio2$beta),
      baseline_beta = c(
        base2$beta[base2$outcome_clean == "BankShare (counts)"] %||% NA_real_,
        base2$beta[base2$outcome_clean == "BankCardRatio (counts)"] %||% NA_real_
      )
    ) |>
      tidyr::pivot_longer(cols = c(baseline_beta, placebo_beta),
                         names_to = "spec", values_to = "beta") |>
      dplyr::mutate(
        spec = dplyr::recode(spec,
                             baseline_beta = "Baseline (Post=2018+)",
                             placebo_beta  = paste0("Placebo (Post=", placebo_year, "+)"))
      )

    p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = outcome, y = beta, shape = spec)) +
      ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
      ggplot2::geom_point(size = 2) +
      ggplot2::labs(
        title = "Baseline vs placebo DiD estimates (EU × Post)",
        subtitle = paste0("Placebo assigns Post=1 for year ≥ ", placebo_year, ". Outcomes based on counts."),
        x = NULL,
        y = "Estimated effect (β)",
        shape = NULL
      ) +
      ggplot2::theme_minimal(base_size = 12)

    ggplot2::ggsave(out_fig, p, width = 7.5, height = 4.5, dpi = 300)
    cat(" - Figure:        ", out_fig, "\n", sep = "")

  } else {
    cat("Baseline raw file found but unexpected structure; skipping baseline-vs-placebo figure.\n")
  }

} else {
  cat("No baseline raw output file found; skipping baseline-vs-placebo figure.\n")
}

cat("\n17e_placebo_did.R complete.\n")