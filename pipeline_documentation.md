# PSD2 Master Thesis — Reproducible Code Pipeline

This repository contains the full, reproducible empirical pipeline for the master’s thesis:

**“Did PSD2 Increase the Adoption of Bank-Based Payments Relative to Card Payments in Europe?”**  
Stockholm University — Department of Economics  
Master’s Programme in Economics (Econometrics track)

The project evaluates the association between PSD2 (implemented from 2018) and payment composition using harmonised country–year panel data and a difference-in-differences design.

---

## Project philosophy

- Scripts are ordered by **conceptual role**, not convenience.
- **Raw data are never modified.**
- Intermediate outputs are **instrument- and source-specific**.
- Processed data are **analysis-ready**.
- **Baseline results are immutable once locked.**
- Robustness extensions are implemented via **parallel scripts** (namespaced).
- The full pipeline is reproducible using `renv`.

---

## Folder structure (overview)

data/
├── raw/
│   ├── ecb/
│   ├── bis/
│   ├── bis_with_CH/
│   └── controls/
│       ├── worldbank/
│       ├── eurostat/
│       └── oecd/
├── intermediate/
│   ├── ecb/
│   ├── bis/
│   ├── bis_with_CH/
│   └── controls/
├── processed/
│   ├── ecb/
│   ├── bis/
│   ├── bis_with_CH/
│   ├── controls/
│   └── panel/
outputs/
├── tables/
├── figures/
└── logs/
scripts/

---

Script execution order

Run scripts top-to-bottom within each stage.

Baseline pipeline and robustness branches are intentionally separated.

---

🟦 STAGE 0 — Project Setup

00_setup.R

Purpose

Load packages

Activate renv

Configure here()

Create folder structure

Outputs

None

---

🟦 STAGE 1 — ECB Pipeline (EU Treated Units)

Credit Transfers

01_ecb_ct_counts_import_tidy.R  
01_ecb_ct_values_import_tidy.R  
03_ecb_ct_merge_counts_values.R  

Direct Debits

04_ecb_dd_counts_import_tidy.R  
04_ecb_dd_values_import_tidy.R  
05_ecb_dd_merge_counts_values.R  

Card Payments

06_ecb_card_counts_import_tidy.R  
06_ecb_card_values_import_tidy.R  
07_ecb_card_merge_counts_values.R  

Outputs

data/intermediate/ecb/  
data/processed/ecb/

---

🟦 STAGE 2 — BIS Pipeline (Baseline Controls: US, CA)

Credit Transfers

21_bis_ct_counts_import_tidy.R  
22_bis_ct_values_import_tidy.R  
23_bis_ct_merge_counts_values.R  

Direct Debits

24_bis_dd_counts_import_tidy.R  
25_bis_dd_values_import_tidy.R  
26_bis_dd_merge_counts_values.R  

Card Payments

27_bis_card_counts_import_tidy.R  
28_bis_card_values_import_tidy.R  
29_bis_card_merge_counts_values.R  

Outputs

data/intermediate/bis/  
data/processed/bis/

---

🟦 STAGE 3 — Control Variables

10_gdppc_import_tidy.R — GDP per capita (PPP)  
11_broadband_import_tidy.R — Broadband penetration  
12_ecomm_import_tidy.R — E-commerce adoption (diagnostic only)

Outputs

data/processed/controls/

---

🟦 STAGE 4 — Master Panel Construction

This stage intentionally separates baseline panel construction from the controls-validated panel.

14_build_master_panel.R

Purpose

Merge ECB payment data  
Merge BIS control countries (US, CA)  
Construct harmonised payment panel  
No estimation-sample restrictions  

Outputs

data/processed/panel/master_panel_payments_country_year.(rds|csv)  
outputs/tables/coverage_master_panel_payments.csv  
outputs/logs/14_build_master_panel_payments.log  

15_build_master_panel_with_controls.R

Purpose

Merge GDPpc and broadband controls  
Validate coverage  
Document missingness  
Create analysis-ready panel for Stage 5  

Important Sample Restriction

Because controls are missing for:

Denmark (DK)  
Luxembourg (LU)  
Malta (MT)  

Controlled specifications use:

24 EU countries  
US and Canada  
Years 2012–2023  

Outputs

data/processed/panel/master_panel_payments_controls_country_year.(rds|csv)  
outputs/tables/coverage_master_panel_payments_controls.csv  

---

🟦 STAGE 5 — Descriptive Diagnostics + Main Econometric Results

This stage produces the core thesis figures and tables.

16_descriptives_and_trends.R

Purpose

Pre-trends diagnostics (EU vs control)  
Indexed and level plots (2012–2017 focus)  

Outputs

Fig16_1_pretrends_levels_bankshare_ct.png  
Fig16_2_pretrends_levels_bankcardratio_ct.png  
Fig16_3_pretrends_indexed_bankshare_ct.png  
Fig16_4_pretrends_indexed_bankcardratio_ct.png  

---

16b_raw_means_diagnostics.R

Purpose

Raw means (no regression)  
EU vs control levels (2012–2023)  
Within-EU heterogeneity  
Country-level post–pre mean changes  

Outputs (Figures)

Fig16b_1_raw_means_bankshare.png  
Fig16b_2_raw_means_bankcardratio.png  
Fig16b_3_eu_country_trajectories_bankshare.png  
Fig16b_4_country_changes_bankshare_ct.png  
Fig16b_5_country_changes_bankcardratio_ct.png  

Outputs (Tables)

tab16b_raw_means_by_group.csv  
tab16b_country_changes_pre_post.csv  

---

17_baseline_did.R

Purpose

Baseline DiD (counts-based)  
Country + year fixed effects  
Clustered SE  
Wild cluster bootstrap inference  

Outputs

tab17_baseline_did_counts_latex.tex  

---

17b_baseline_did_controls.R

Purpose

DiD + controls  
Coefficient stability  

Outputs

tab17b_baseline_did_controls_latex.tex  
Fig17b_beta_stability_controls.png  

---

17f_trend_augmented_did.R

Purpose

Trend-augmented DiD specification  
Adds country-specific linear time trends  
Tests whether baseline estimates reflect differential pre-trends  

Outputs

tab17f_trend_augmented_did_latex.tex  
tab17f_trend_augmented_did_raw.csv  

---

17g_compare_baseline_vs_trend_augmented.R

Purpose

Direct comparison of baseline and trend-augmented DiD estimates  
Highlights attenuation of coefficients when accounting for country-specific trends  
Used for main interpretation of identification  

Outputs

tab17g_baseline_vs_trend_augmented_latex.tex  
tab17g_baseline_vs_trend_augmented_raw.csv  

---

17e_placebo_did.R

Purpose

Placebo DiD diagnostic using fictitious treatment year (PostPlacebo = 1 for years ≥ 2016)  
Tests whether baseline estimates could be driven by spurious trend differences  
Estimation sample, fixed effects structure, clustering, and wild bootstrap inference identical to baseline  

Outputs

tab17e_placebo_did_latex.tex  
tab17e_placebo_did_raw.csv  
Fig17e_placebo_vs_baseline_beta.png  

---

18_event_study.R

Purpose

Dynamic event-time specification  
Diagnostic complement to baseline DiD  

Outputs

tab18_eventstudy_coefs_slim.csv  
Fig18_1_eventstudy_bankshare_ct.png  
Fig18_2_eventstudy_bankcardratio_ct.png  

---

19_values_did.R & 19b_values_did_controls.R

Purpose

Measurement robustness using transaction values  

Outputs

tab19_values_did_latex.tex  
tab19b_values_did_controls_latex.tex  
Fig19_values_did_beta.png  
Fig19b_values_beta_stability_controls.png  
---

🟦 STAGE 6 — Robustness Branch: Switzerland (CH)

This branch does not modify baseline outputs.

Includes CH as additional control country.

Scripts

21b–29b BIS imports (with CH)  
14b_build_master_panel_with_CH.R  
17c / 17d DiD with CH  
99_check_with_CH_superset_invariance.R  

Outputs

Parallel datasets with _with_CH suffix.

Tables:

tab17c_baseline_did_counts_latex_with_CH.tex  
tab17d_baseline_did_controls_latex_with_CH.tex  

Figures:

Fig17c_baseline_did_beta_with_CH.png  
Fig17d_beta_stability_controls_with_CH.png  

---

🟦 STAGE 7 — Definition Robustness (Direct Debits)

19c_dd_robustness.R

Purpose

Redefine bank-based outcome to include DD  
Restrict sample to 2022–2024  
Diagnostic robustness only  

Outputs

tab19c_dd_robustness_latex.tex  
Fig19c_dd_robustness_beta.png  

---

🟦 STAGE 8 — Conceptual Heterogeneity (Appendix Input)

18b_baseline_exposure_2015.R

Purpose

Construct 2015 card exposure  
Median split (High vs Low)  
Input for potential DDD extension  

Outputs

tab18b_baseline_card_exposure_2015.csv  

---

🟦 STAGE 9 — Final Export / Examiner Packaging

20_export_tables_figures.R

Purpose

Copy thesis-included artifacts to:

outputs/final/

Generate manifest  
Create examiner-facing README  

Outputs

outputs/final/tables/  
outputs/final/figures/  
outputs/final/logs/  
manifest.csv  

README_FINAL_EXPORT.txt  

---

⚠ Notes on Data Limitations

Direct debit data are short and unbalanced → not baseline.  

Values-based outcomes more volatile → complementary only.  

Controlled specifications restrict EU sample (DK, LU, MT excluded).

---

🔒 Reproducibility Guarantees

Baseline scripts are not modified after lock-in.  

Robustness branches are additive and namespaced.  

renv.lock fixes package versions.  

Pipeline can be rerun end-to-end from raw data.
