rm(list = ls())

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

assert_no_diff_overlap_tol <- function(old, new, keys, measures, label, tol = 1e-6) {

  old2 <- old %>%
    select(all_of(c(keys, measures))) %>%
    arrange(across(all_of(keys)))

  new2 <- new %>%
    semi_join(old2 %>% select(all_of(keys)), by = keys) %>%
    select(all_of(c(keys, measures))) %>%
    arrange(across(all_of(keys)))

  # same keys
  stopifnot(nrow(old2) == nrow(new2))
  stopifnot(identical(old2 %>% select(all_of(keys)), new2 %>% select(all_of(keys))))

  # join and compare with tolerance
  j <- old2 %>%
    rename_with(~ paste0(.x, "_old"), all_of(measures)) %>%
    left_join(
      new2 %>% rename_with(~ paste0(.x, "_new"), all_of(measures)),
      by = keys
    )

  # build a logical flag for each measure
  for (m in measures) {
    oldm <- paste0(m, "_old")
    newm <- paste0(m, "_new")
    diffm <- paste0(m, "_absdiff")

    j <- j %>%
      mutate(
        !!diffm := dplyr::case_when(
          is.na(.data[[oldm]]) & is.na(.data[[newm]]) ~ 0,
          xor(is.na(.data[[oldm]]), is.na(.data[[newm]])) ~ Inf,
          TRUE ~ abs(.data[[oldm]] - .data[[newm]])
        )
      )
  }

  # any measure exceeds tolerance?
  diff_cols <- paste0(measures, "_absdiff")
  bad <- j %>%
    filter(if_any(all_of(diff_cols), ~ .x > tol))

  cat("\n---", label, "---\n")
  cat("Overlap rows checked:", nrow(j), "\n")
  cat("Tolerance:", tol, "\n")
  cat("Rows with abs diff > tol (or NA mismatch):", nrow(bad), "\n")

  if (nrow(bad) > 0) {
    cat("\nShowing up to 20 problematic rows with deltas:\n")
    show_cols <- c(keys,
                   as.vector(rbind(paste0(measures, "_old"),
                                   paste0(measures, "_new"),
                                   paste0(measures, "_absdiff"))))
    print(head(bad %>% select(all_of(show_cols)), 20))
    stop("FAIL: Overlap cells differ beyond tolerance for ", label)
  } else {
    cat("PASS: Overlap cells identical within tolerance for ", label, "\n")
  }
}

# ---- CT (credit transfers) ----
ct_old_path <- "data/processed/bis/credit_transfers/ct_country_year.rds"
ct_new_path <- "data/processed/bis_with_CH/credit_transfers/ct_country_year_with_CH.rds"
stopifnot(file.exists(ct_old_path), file.exists(ct_new_path))

ct_old <- readRDS(ct_old_path) %>% filter(country %in% c("US", "CA"))
ct_new <- readRDS(ct_new_path) %>% filter(country %in% c("US", "CA", "CH"))

assert_no_diff_overlap_tol(
  old = ct_old,
  new = ct_new,
  keys = c("country", "year"),
  measures = c("ct_sent_count", "ct_sent_value"),
  label = "CT merged (US/CA overlap)",
  tol = 1e-6
)

# ---- CP (card payments) ----
cp_old_path <- "data/processed/bis/card_payments/cp_country_year.rds"
cp_new_path <- "data/processed/bis_with_CH/card_payments/cp_country_year_with_CH.rds"
stopifnot(file.exists(cp_old_path), file.exists(cp_new_path))

cp_old <- readRDS(cp_old_path) %>% filter(country %in% c("US", "CA"))
cp_new <- readRDS(cp_new_path) %>% filter(country %in% c("US", "CA", "CH"))

assert_no_diff_overlap_tol(
  old = cp_old,
  new = cp_new,
  keys = c("country", "year"),
  measures = c("cp_count", "cp_value"),
  label = "CP merged (US/CA overlap)",
  tol = 1e-6
)

cat("\nAll checks passed ✅ with_CH processed files are safe supersets of baseline (within tolerance).\n")
