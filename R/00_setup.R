# scripts/00_setup.R
# ============================================================
# Project setup: packages, paths, and folder structure
# ============================================================

# Guard: only run setup once per R session -------------------------------
if (exists(".PSD2_SETUP_DONE", envir = .GlobalEnv) &&
    isTRUE(get(".PSD2_SETUP_DONE", envir = .GlobalEnv))) {
  invisible(TRUE)

} else {

  # Load core packages (quiet) -------------------------------------------
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(readr)
    library(stringr)
    library(ggplot2)
    library(scales)
    library(here)

    # Optional (used later if needed)
    library(lubridate)
    library(readxl)
  })

  # Set project root ------------------------------------------------------
  # Only call i_am() if not already set in this session
  if (is.null(getOption("here.files"))) {
    here::i_am("scripts/00_setup.R")
  }

# renv handling ---------------------------------------------------------
# IMPORTANT: Do NOT call renv::activate() inside setup scripts.
# renv is activated automatically when you open the project / start R in the project.
# Calling activate() inside a script is what causes the "please restart R session" loop.

if (!requireNamespace("renv", quietly = TRUE)) {
  message("Note: 'renv' is not installed. Install it with install.packages('renv') for full reproducibility.")
}


  # Check project root ---------------------------------------------------
  stopifnot(
    file.exists(here("README.md")),
    file.exists(here("renv.lock")),
    file.exists(here("scripts/00_setup.R"))
  )

  # Create required folder structure ------------------------------------
  dirs <- c(
    # raw
    "data/raw/ecb", "data/raw/bis", "data/raw/controls",
    # intermediate
    "data/intermediate/ecb", "data/intermediate/bis", "data/intermediate/controls",
    # processed
    "data/processed/ecb", "data/processed/bis", "data/processed/controls",
    "data/processed/panel", "data/processed/treatments",
    # outputs
    "outputs/tables", "outputs/figures", "outputs/logs"
  )

  for (d in dirs) {
    dir.create(here(d), recursive = TRUE, showWarnings = FALSE)
  }

  # Set global options ---------------------------------------------------
  options(
    stringsAsFactors = FALSE,
    scipen = 999
  )

  # Save session info ----------------------------------------------------
  writeLines(c(
    paste0("Timestamp: ", Sys.time()),
    R.version.string,
    capture.output(sessionInfo())
  ), here("outputs/logs/sessionInfo.txt"))

  message("Project setup complete.")

  # Mark setup as done ---------------------------------------------------
  assign(".PSD2_SETUP_DONE", TRUE, envir = .GlobalEnv)
}

