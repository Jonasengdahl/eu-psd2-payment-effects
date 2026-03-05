# scripts/20_export_tables_figures.R
# ============================================================
# Final export / packaging script (NO estimation).
#
# Purpose:
#   - Collect ONLY the tables/figures/logs used in the thesis
#   - Copy them into outputs/final/{tables,figures,logs}
#   - Write a manifest for examiner-facing reproducibility
#
# This script should be run LAST, after all analysis scripts.
# ============================================================

source(here::here("scripts/00_setup.R"))

cat("============================================================\n")
cat("20_export_tables_figures.R\n")
cat("Timestamp: ", as.character(Sys.time()), "\n")
cat("============================================================\n\n")

# ---- Settings ------------------------------------------------------------

export_root <- here::here("outputs/final")
dir_tables  <- file.path(export_root, "tables")
dir_figs    <- file.path(export_root, "figures")
dir_logs    <- file.path(export_root, "logs")

# IMPORTANT:
# List ONLY the artifacts you actually include in the thesis
# (Add/remove items here as the thesis locks in.)
final_tables <- c(
  # Baseline (counts)
  "outputs/tables/tab17_baseline_did_counts_latex.tex",

  # CH controls table used in thesis (non-TRIM file that exists)
  "outputs/tables/tab17d_baseline_did_controls_latex_with_CH.tex",

  # Placebo DiD
  "outputs/tables/tab17e_placebo_did_latex.tex",

  # Event study coefficients (slim CSV used in Overleaf)
  "outputs/tables/tab18_eventstudy_coefs_slim.csv",

  # Baseline exposure (conceptual extension)
  "outputs/tables/tab18b_baseline_card_exposure_2015.csv",

  # Values robustness
  "outputs/tables/tab19_values_did_latex.tex",
  "outputs/tables/tab19b_values_did_controls_latex.tex",

  # DD robustness
  "outputs/tables/tab19c_dd_robustness_latex.tex"
)

final_figures <- c(
  # Pre-trends (Stage 16)
  "outputs/figures/Fig16_1_pretrends_levels_bankshare_ct.png",
  "outputs/figures/Fig16_2_pretrends_levels_bankcardratio_ct.png",
  "outputs/figures/Fig16_3_pretrends_indexed_bankshare_ct.png",
  "outputs/figures/Fig16_4_pretrends_indexed_bankcardratio_ct.png",

  # Raw means + within-EU descriptives (Stage 16b)
  "outputs/figures/Fig16b_1_raw_means_bankshare.png",
  "outputs/figures/Fig16b_2_raw_means_bankcardratio.png",
  "outputs/figures/Fig16b_3_eu_country_trajectories_bankshare.png",
  "outputs/figures/Fig16b_4_country_changes_bankshare_ct.png",
  "outputs/figures/Fig16b_5_country_changes_bankcardratio_ct.png",

  # Baseline beta plot
  "outputs/figures/Fig17_baseline_did_beta.png",

  # Controls stability plot (counts)
  "outputs/figures/Fig17b_beta_stability_controls.png",

  # CH robustness figures
  "outputs/figures/Fig17c_baseline_did_beta_with_CH.png",
  "outputs/figures/Fig17d_beta_stability_controls_with_CH.png",

  # Placebo vs baseline comparison figure
  "outputs/figures/Fig17e_placebo_vs_baseline_beta.png",

  # Event study figures
  "outputs/figures/Fig18_1_eventstudy_bankshare_ct.png",
  "outputs/figures/Fig18_2_eventstudy_bankcardratio_ct.png",

  # Values figures
  "outputs/figures/Fig19_values_did_beta.png",
  "outputs/figures/Fig19b_values_beta_stability_controls.png",

  # DD robustness figure
  "outputs/figures/Fig19c_dd_robustness_beta.png"
)

final_logs <- c(
  # If you want to include “proof” logs:
  "outputs/logs/14_build_master_panel_payments.log",
  "outputs/logs/18_event_study.log"
)

# ---- Helpers ------------------------------------------------------------

ensure_dir <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
}

copy_one <- function(rel_path, out_dir) {
  in_path <- here::here(rel_path)
  if (!file.exists(in_path)) {
    warning("Missing file (skipping): ", rel_path)
    return(NULL)
  }
  out_path <- file.path(out_dir, basename(rel_path))
  ok <- file.copy(from = in_path, to = out_path, overwrite = TRUE)
  if (!ok) stop("Failed to copy: ", rel_path)
  info <- file.info(in_path)
  data.frame(
    type = basename(out_dir),
    file = basename(rel_path),
    source_path = rel_path,
    exported_path = file.path("outputs/final", basename(out_dir), basename(rel_path)),
    modified = as.character(info$mtime),
    stringsAsFactors = FALSE
  )
}

# ---- Create export folders ----------------------------------------------

cat("Creating export folders...\n")
ensure_dir(export_root)
ensure_dir(dir_tables)
ensure_dir(dir_figs)
ensure_dir(dir_logs)

# ---- Copy artifacts ------------------------------------------------------

cat("Copying final tables...\n")
man_tables <- do.call(rbind, lapply(final_tables, copy_one, out_dir = dir_tables))

cat("Copying final figures...\n")
man_figs <- do.call(rbind, lapply(final_figures, copy_one, out_dir = dir_figs))

cat("Copying final logs...\n")
man_logs <- do.call(rbind, lapply(final_logs, copy_one, out_dir = dir_logs))

manifest <- dplyr::bind_rows(man_tables, man_figs, man_logs)

# ---- Save manifest + README ---------------------------------------------

manifest_path <- file.path(export_root, "manifest.csv")
readme_path   <- file.path(export_root, "README_FINAL_EXPORT.txt")

readr::write_csv(manifest, manifest_path)

writeLines(c(
  "PSD2 Master Thesis — FINAL EXPORT",
  "=================================",
  "",
  "This folder contains the final tables/figures/logs used in the submitted thesis PDF.",
  "Generated by: scripts/20_export_tables_figures.R",
  paste0("Timestamp: ", as.character(Sys.time())),
  "",
  "Subfolders:",
  " - tables/   : LaTeX fragments + CSV tables used by Overleaf",
  " - figures/  : PNG figures included in the thesis",
  " - logs/     : selected logs (optional) demonstrating reproducible runs",
  "",
  "Manifest:",
  " - manifest.csv lists all exported artifacts and their original source paths."
), con = readme_path)

# ---- Summary ------------------------------------------------------------

cat("\nExport complete.\n")
cat("Export root:   ", export_root, "\n")
cat("Manifest:      ", manifest_path, "\n")
cat("Tables copied: ", ifelse(is.null(man_tables), 0, nrow(man_tables)), "\n")
cat("Figures copied:", ifelse(is.null(man_figs), 0, nrow(man_figs)), "\n")
cat("Logs copied:   ", ifelse(is.null(man_logs), 0, nrow(man_logs)), "\n")

# Also print any missing files warnings clearly at the end:
missing_any <- any(!file.exists(here::here(c(final_tables, final_figures, final_logs))))
if (missing_any) {
  cat("\nNOTE: Some files were missing and were skipped. Check warnings above.\n")
} else {
  cat("\nAll requested artifacts were found and exported.\n")
}