# EU PSD2 Payment Effects

This project evaluates whether the EU's Second Payment Services Directive (PSD2), implemented in 2018, increased the adoption of bank-based payments relative to card payments.

The analysis uses cross-country panel data (2012–2023) and a difference-in-differences design comparing EU countries with non-EU control countries.

This repository contains the full reproducible empirical pipeline for the master's thesis:

"Did PSD2 Increase the Adoption of Bank-Based Payments Relative to Card Payments in Europe?"
Stockholm University — MSc Economics (Econometrics Track)


## Research Question

Did PSD2 increase the use of bank-based payments relative to card payments in Europe?


## Identification Strategy

The empirical strategy uses a difference-in-differences framework:

Y_ct = α + β(EU_c × Post_t) + μ_c + τ_t + ε_ct

EU_c = EU country indicator  
Post_t = post-2018 period

Outcome variables:

• BankShare = credit transfers / (credit transfers + card payments)  
• BankCardRatio = credit transfers / card payments


## Main Results

Baseline difference-in-differences estimates suggest that PSD2 is associated with an increase in the share of bank-based payments relative to card payments.

Event-study specifications indicate that the shift emerges gradually after implementation.

## Key Empirical Results

### Event Study

![Event Study](figures/Fig18_1_eventstudy_bankshare_ct.png)

Dynamic treatment effects relative to the PSD2 implementation year.

### Pre-Trends Diagnostic

![Pretrends](figures/Fig16_3_pretrends_indexed_bankshare_ct.png)

Indexed payment shares for EU and control countries prior to PSD2.

### Baseline DiD Estimates

![DiD](figures/Fig17b_beta_stability_controls.png)

Coefficient stability across baseline and controlled specifications.


## Repository Structure

scripts/        empirical pipeline
data/           raw and processed datasets
outputs/        figures and tables


## Reproducibility

The entire pipeline can be executed sequentially using the scripts in the `scripts/` folder.

Package versions are managed using `renv`.


## Full Pipeline Documentation

A detailed description of the full empirical pipeline is available in:

pipeline_documentation.md
