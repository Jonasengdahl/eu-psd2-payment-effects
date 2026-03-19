# scripts/17g_compare_baseline_vs_trend_augmented.R
# ============================================================
# Compare baseline vs trend-augmented DiD estimates
#
# Purpose:
#   - Create side-by-side comparison table using raw outputs from:
#       * 17b_baseline_did_controls.R
#       * 17f_trend_augmented_did.R
#   - No re-estimation is performed here
#   - Produces thesis-ready comparison table with 6 columns:
#       (1) Baseline: No controls
#       (2) Baseline: + log GDPpc
#       (3) Baseline: + log GDPpc + log(1+broadband)
#       (4) Trend: No controls
#       (5) Trend: + log GDPpc
#       (6) Trend: + log GDPpc + log(1+broadband)
#
# Outcomes:
#   Panel A: BankShare_ct
#   Panel B: BankCardRatio_ct
#
# Inputs:
#   - outputs/tables/tab17b_baseline_did_controls_raw.csv
#   - outputs/tables/tab17f_trend_augmented_did_raw.csv
#
# Outputs:
#   - outputs/tables/tab17g_baseline_vs_trend_augmented_raw.csv
#   - outputs/tables/tab17g_baseline_vs_trend_augmented_latex.tex
#   - outputs/logs/17g_compare_baseline_vs_trend_augmented.log
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

log_file <- here::here("outputs/logs/17g_compare_baseline_vs_trend_augmented.log")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
on.exit({
  while (sink.number() > 0) sink()
  close(log_con)
}, add = TRUE)

cat("\n=========================================\n")
cat("17g_compare_baseline_vs_trend_augmented.R — START\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n\n")

# -----------------------------
# 1) Paths
# -----------------------------
print_section("1. Paths")

in_baseline <- here::here("outputs/tables/tab17b_baseline_did_controls_raw.csv")
in_trend    <- here::here("outputs/tables/tab17f_trend_augmented_did_raw.csv")

out_raw <- here::here("outputs/tables/tab17g_baseline_vs_trend_augmented_raw.csv")
out_tex <- here::here("outputs/tables/tab17g_baseline_vs_trend_augmented_latex.tex")

cat("Inputs:\n")
cat(" - ", in_baseline, "\n", sep = "")
cat(" - ", in_trend, "\n\n", sep = "")

cat("Outputs:\n")
cat(" - ", out_raw, "\n", sep = "")
cat(" - ", out_tex, "\n", sep = "")
cat(" - ", log_file, "\n\n", sep = "")

stopifnot(file.exists(in_baseline))
stopifnot(file.exists(in_trend))

# -----------------------------
# 2) Load raw results
# -----------------------------
print_section("2. Load raw results")

base <- readr::read_csv(in_baseline, show_col_types = FALSE)
trend <- readr::read_csv(in_trend, show_col_types = FALSE)

cat("Baseline rows: ", nrow(base), "\n", sep = "")
cat("Trend rows:    ", nrow(trend), "\n\n", sep = "")

# -----------------------------
# 3) Harmonise and stack
# -----------------------------
print_section("3. Harmonise and stack")

req_cols <- c("outcome", "spec", "beta", "se_cluster", "p_wild", "n_obs", "n_countries")
missing_base  <- setdiff(req_cols, names(base))
missing_trend <- setdiff(req_cols, names(trend))

if (length(missing_base) > 0) {
  stop("Baseline raw file is missing columns: ", paste(missing_base, collapse = ", "))
}
if (length(missing_trend) > 0) {
  stop("Trend raw file is missing columns: ", paste(missing_trend, collapse = ", "))
}

base2 <- base %>%
  dplyr::transmute(
    outcome,
    spec,
    model_family = "Baseline",
    col_id = dplyr::case_when(
      spec == "1" ~ "1",
      spec == "2" ~ "2",
      spec == "3" ~ "3",
      TRUE ~ NA_character_
    ),
    col_label = dplyr::case_when(
      spec == "1" ~ "Baseline: No controls",
      spec == "2" ~ "Baseline: + log GDPpc",
      spec == "3" ~ "Baseline: + log GDPpc + log(1+broadband)",
      TRUE ~ NA_character_
    ),
    beta,
    se_cluster,
    p_wild,
    n_obs,
    n_countries
  )

trend2 <- trend %>%
  dplyr::transmute(
    outcome,
    spec,
    model_family = "Trend",
    col_id = dplyr::case_when(
      spec == "1" ~ "4",
      spec == "2" ~ "5",
      spec == "3" ~ "6",
      TRUE ~ NA_character_
    ),
    col_label = dplyr::case_when(
      spec == "1" ~ "Trend: No controls",
      spec == "2" ~ "Trend: + log GDPpc",
      spec == "3" ~ "Trend: + log GDPpc + log(1+broadband)",
      TRUE ~ NA_character_
    ),
    beta,
    se_cluster,
    p_wild,
    n_obs,
    n_countries
  )

combined <- dplyr::bind_rows(base2, trend2) %>%
  dplyr::arrange(outcome, as.integer(col_id))

readr::write_csv(combined, out_raw)

cat("Combined comparison table written to raw CSV.\n\n")

# -----------------------------
# 4) Helpers
# -----------------------------
print_section("4. Formatting helpers")

fmt_num <- function(x, digits = 3) formatC(x, format = "f", digits = digits)
fmt_p <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) "<0.001" else formatC(p, format = "f", digits = 3)
}

get_cell <- function(df, outcome_name, col_id_value, field) {
  out <- df %>%
    dplyr::filter(outcome == outcome_name, col_id == col_id_value) %>%
    dplyr::pull({{field}})
  if (length(out) != 1) {
    stop("Expected exactly one value for outcome=", outcome_name,
         ", col_id=", col_id_value, ", field=", deparse(substitute(field)))
  }
  out
}

# -----------------------------
# 5) Extract cells for LaTeX
# -----------------------------
print_section("5. Extract cells for LaTeX")

cols <- as.character(1:6)

# Panel A: BankShare
b_bs  <- sapply(cols, function(j) fmt_num(get_cell(combined, "BankShare_ct", j, beta)))
se_bs <- sapply(cols, function(j) fmt_num(get_cell(combined, "BankShare_ct", j, se_cluster)))
p_bs  <- sapply(cols, function(j) fmt_p(get_cell(combined, "BankShare_ct", j, p_wild)))

# Panel B: BankCardRatio
b_br  <- sapply(cols, function(j) fmt_num(get_cell(combined, "BankCardRatio_ct", j, beta)))
se_br <- sapply(cols, function(j) fmt_num(get_cell(combined, "BankCardRatio_ct", j, se_cluster)))
p_br  <- sapply(cols, function(j) fmt_p(get_cell(combined, "BankCardRatio_ct", j, p_wild)))

# Shared sample stats (should match across all columns)
n_obs_bs <- sapply(cols, function(j) get_cell(combined, "BankShare_ct", j, n_obs))
n_cty_bs <- sapply(cols, function(j) get_cell(combined, "BankShare_ct", j, n_countries))

# -----------------------------
# 6) Write LaTeX fragment
# -----------------------------
print_section("6. Write LaTeX fragment")

latex_lines <- c(
  "% Auto-generated by scripts/17g_compare_baseline_vs_trend_augmented.R",
  "% Six-column comparison: baseline vs trend-augmented DiD",
  "% Columns 1-3 = Baseline, Columns 4-6 = Trend-augmented",
  "% Panel A: BankShare",
  "\\midrule",
  "\\multicolumn{7}{l}{\\textit{Panel A. BankShare (counts)}} \\\\",
  paste0(
    "EU $\\times$ Post(2018+) & \\textbf{",
    paste(b_bs, collapse = "} & \\textbf{"),
    "} \\\\"
  ),
  paste0(
    "& (",
    paste(se_bs, collapse = ") & ("),
    ") \\\\"
  ),
  paste0(
    "Wild bootstrap p-value & ",
    paste(p_bs, collapse = " & "),
    " \\\\"
  ),
  "\\addlinespace",
  "% Panel B: BankCardRatio",
  "\\multicolumn{7}{l}{\\textit{Panel B. BankCardRatio (counts)}} \\\\",
  paste0(
    "EU $\\times$ Post(2018+) & \\textbf{",
    paste(b_br, collapse = "} & \\textbf{"),
    "} \\\\"
  ),
  paste0(
    "& (",
    paste(se_br, collapse = ") & ("),
    ") \\\\"
  ),
  paste0(
    "Wild bootstrap p-value & ",
    paste(p_br, collapse = " & "),
    " \\\\"
  ),
  "\\midrule",
  paste0("Observations & ", paste(n_obs_bs, collapse = " & "), " \\\\"),
  paste0("Countries & ", paste(n_cty_bs, collapse = " & "), " \\\\"),
  "Country FE & Yes & Yes & Yes & Yes & Yes & Yes \\\\",
  "Year FE & Yes & Yes & Yes & Yes & Yes & Yes \\\\",
  "Country trends & No & No & No & Yes & Yes & Yes \\\\",
  "Controls & No & log GDPpc & log GDPpc + log(1+broadband) & No & log GDPpc & log GDPpc + log(1+broadband) \\\\"
)

writeLines(latex_lines, out_tex)

# -----------------------------
# 7) Console preview
# -----------------------------
print_section("7. Console preview")

cat("Panel A: BankShare_ct\n")
print(combined %>% dplyr::filter(outcome == "BankShare_ct"))
cat("\n")

cat("Panel B: BankCardRatio_ct\n")
print(combined %>% dplyr::filter(outcome == "BankCardRatio_ct"))
cat("\n")

# -----------------------------
# 8) End
# -----------------------------
print_section("8. Complete")

cat("17g_compare_baseline_vs_trend_augmented.R complete.\n\n")
cat("Saved outputs:\n")
cat(" - Raw comparison CSV: ", out_raw, "\n", sep = "")
cat(" - LaTeX fragment:     ", out_tex, "\n", sep = "")
cat(" - Log file:           ", log_file, "\n\n", sep = "")

cat("\n=========================================\n")
cat("17g_compare_baseline_vs_trend_augmented.R — END\n")
cat("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=========================================\n")