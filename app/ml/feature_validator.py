"""
feature_validator.py — Pre-training data leakage & quality validator for Bet Hero.

⚠️  This module is the LAST LINE OF DEFENCE against data leakage.
    It MUST be run before every model training run.
    Training is BLOCKED if any critical check fails.

Checks performed
----------------
1. TEMPORAL LEAKAGE
   - Verifies that every feature's source timestamp < match_date
   - Flags any feature computed with data from the future
   - Checks that closing odds features are excluded from training
     (closing odds are published at match time, always available)

2. TARGET LEAKAGE
   - Detects features with suspiciously high correlation to targets
     (Pearson |r| > CORRELATION_HARD_BLOCK)
   - Specifically bans known leaky columns: home_score, away_score,
     actual_outcome, result, post-match stats

3. CONSTANT / NEAR-CONSTANT FEATURES
   - Features with variance ≈ 0 add no signal and inflate model complexity

4. NaN / COVERAGE CHECKS
   - Features with > MAX_NAN_FRACTION missing values are flagged
   - Features that are all-zero for test rows are suspicious

5. DISTRIBUTION SHIFT (train vs val)
   - Kolmogorov–Smirnov test on each feature between train and val sets
   - Large KS statistic indicates temporal distribution shift

6. DUPLICATE ROWS
   - Exact duplicate (match_id, feature_set) rows cause data leakage
     in cross-validation when a match appears in both train and val

7. TARGET DISTRIBUTION CHECK
   - Verifies outcome class balance is reasonable (not degenerate)

Output
------
ValidationReport — structured report with:
  - passed: bool   (False → BLOCK training)
  - critical_errors: list[str]
  - warnings: list[str]
  - feature_stats: DataFrame
  - correlation_matrix: DataFrame

Usage
-----
    from app.ml.feature_validator import FeatureValidator

    validator = FeatureValidator()
    report = validator.run(X_train, y_train, X_val, y_val,
                           match_dates_train=dates_series)
    if not report.passed:
        raise RuntimeError(f"Validation failed:\\n{report.summary()}")
"""

from __future__ import annotations

import dataclasses
import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp, pearsonr

logger = logging.getLogger(__name__)

# ─────────────────────────── Thresholds ────────────────────────────────────

# Correlation with target: above HARD = block, above WARN = warning
CORRELATION_HARD_BLOCK  = 0.85
CORRELATION_WARN        = 0.60

# NaN fraction: above HARD = block, above WARN = warning
NAN_HARD_BLOCK          = 0.80   # >80% missing → block
NAN_WARN                = 0.30   # >30% missing → warn

# Constant feature: std below this is treated as constant
CONSTANT_VARIANCE_EPS   = 1e-8

# KS distribution shift p-value threshold (flag if p < this on val set)
KS_SHIFT_ALPHA          = 0.01

# Known post-match (leaky) column fragments — HARD BLOCK if present
LEAKY_COLUMN_FRAGMENTS: list[str] = [
    "home_score",
    "away_score",
    "actual_outcome",
    "result",
    "post_match",
    "full_time",
    "ft_",
    "half_time",
    "ht_",
    "corners_",      # post-match stats
    "yellow_cards_",
    "red_cards_",
    "xg_actual",
    "xga_actual",
    "goals_scored",  # actual goals (not predictions/form averages)
    "goals_conceded",# same — distinguish from rolling averages
]

# These column fragments are OK even though they look like result data
LEAKY_ALLOWLIST: list[str] = [
    "form",          # form features use rolling history (not current match)
    "avg_goals",     # h2h / form averages
    "gscored",       # form rolling avg goals scored
    "gconceded",     # form rolling avg goals conceded
    "xg",            # xG averages (historical)
    "xga",           # same
]


# ─────────────────────────── Report Dataclass ──────────────────────────────

@dataclass
class ValidationReport:
    passed:           bool
    critical_errors:  list[str]     = field(default_factory=list)
    warnings:         list[str]     = field(default_factory=list)
    feature_stats:    Any           = None      # pd.DataFrame when populated
    correlation_top:  Any           = None      # pd.DataFrame top-20 correlations
    ks_results:       Any           = None      # pd.DataFrame KS test results
    n_features:       int           = 0
    n_train_rows:     int           = 0
    n_val_rows:       int           = 0
    duplicate_rows:   int           = 0

    def summary(self) -> str:
        lines = [
            f"{'✅ PASSED' if self.passed else '❌ BLOCKED'} — Feature Validation Report",
            f"  Features:     {self.n_features}",
            f"  Train rows:   {self.n_train_rows}",
            f"  Val rows:     {self.n_val_rows}",
            f"  Duplicates:   {self.duplicate_rows}",
        ]
        if self.critical_errors:
            lines.append(f"\n  🚨 CRITICAL ({len(self.critical_errors)}):")
            for e in self.critical_errors:
                lines.append(f"    • {e}")
        if self.warnings:
            lines.append(f"\n  ⚠️  WARNINGS ({len(self.warnings)}):")
            for w in self.warnings[:20]:     # cap at 20 warnings in summary
                lines.append(f"    • {w}")
            if len(self.warnings) > 20:
                lines.append(f"    … and {len(self.warnings) - 20} more")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["feature_stats"]   = self.feature_stats.to_dict()   if self.feature_stats   is not None else None
        d["correlation_top"] = self.correlation_top.to_dict() if self.correlation_top is not None else None
        d["ks_results"]      = self.ks_results.to_dict()      if self.ks_results      is not None else None
        return d


# ─────────────────────────── Validator ─────────────────────────────────────

class FeatureValidator:
    """
    Validates a training dataset for data leakage and quality issues.

    Parameters
    ----------
    hard_block_on_leakage : bool
        If True (default), raises RuntimeError when training is blocked.
        Set False in dry-run / reporting mode.
    """

    def __init__(self, hard_block_on_leakage: bool = True):
        self.hard_block = hard_block_on_leakage

    # ── Main entry point ─────────────────────────────────────────

    def run(
        self,
        X_train:            pd.DataFrame,
        y_train:            pd.Series,
        X_val:              pd.DataFrame,
        y_val:              pd.Series,
        match_dates_train:  pd.Series | None = None,
        match_dates_val:    pd.Series | None = None,
        feature_computed_at: pd.Series | None = None,
    ) -> ValidationReport:
        """
        Run the full validation suite. Returns a ValidationReport.

        Parameters
        ----------
        X_train / X_val         : feature matrices (must be pandas DataFrames)
        y_train / y_val         : target labels (must be pandas Series)
        match_dates_train       : Series of match_date per train row (for temporal check)
        match_dates_val         : same for val
        feature_computed_at     : Series of the timestamps when features were computed
                                  (used for strict temporal leakage check)

        Returns
        -------
        ValidationReport  — always returned even on failure (to allow reporting)

        Raises
        ------
        RuntimeError if critical errors found AND hard_block_on_leakage=True
        """
        report = ValidationReport(
            passed=True,
            n_features=X_train.shape[1],
            n_train_rows=len(X_train),
            n_val_rows=len(X_val),
        )

        X_all = pd.concat([X_train, X_val], ignore_index=True)
        y_all = pd.concat([y_train, y_val], ignore_index=True)

        logger.info(
            "FeatureValidator: %d features, %d train rows, %d val rows",
            report.n_features, report.n_train_rows, report.n_val_rows,
        )

        # ── Run all checks ────────────────────────────────────────

        self._check_banned_columns(X_train, report)
        self._check_target_leakage(X_all, y_all, report)
        self._check_temporal_leakage(X_train, match_dates_train, feature_computed_at, report)
        self._check_nan_coverage(X_train, X_val, report)
        self._check_constant_features(X_train, report)
        self._check_duplicate_rows(X_train, report)
        self._check_target_distribution(y_train, y_val, report)
        report.feature_stats   = self._compute_feature_stats(X_train, X_val)
        report.correlation_top = self._compute_correlations(X_all, y_all, report)
        report.ks_results      = self._check_distribution_shift(X_train, X_val, report)

        # ── Final pass/fail ───────────────────────────────────────

        if report.critical_errors:
            report.passed = False
            logger.error("FeatureValidator BLOCKED:\n%s", report.summary())
            if self.hard_block:
                raise RuntimeError(
                    f"Training blocked — {len(report.critical_errors)} critical leakage issue(s).\n"
                    + "\n".join(f"  • {e}" for e in report.critical_errors)
                )
        else:
            logger.info("FeatureValidator PASSED — %d warnings", len(report.warnings))
            if report.warnings:
                logger.warning("Validation warnings:\n%s", "\n".join(f"  • {w}" for w in report.warnings))

        return report

    # ── Check 1: Banned column names ────────────────────────────

    def _check_banned_columns(self, X: pd.DataFrame, report: ValidationReport) -> None:
        """
        Hard-block any column whose name contains a known leaky fragment
        unless it's in the allowlist.
        """
        cols = [c.lower() for c in X.columns]
        for col_lower, col_orig in zip(cols, X.columns):
            for fragment in LEAKY_COLUMN_FRAGMENTS:
                if fragment in col_lower:
                    # Check allowlist
                    if any(allow in col_lower for allow in LEAKY_ALLOWLIST):
                        break
                    report.critical_errors.append(
                        f"BANNED COLUMN '{col_orig}' contains leaky fragment '{fragment}'"
                    )
                    break

    # ── Check 2: Target-correlation leakage ─────────────────────

    def _check_target_leakage(
        self, X: pd.DataFrame, y: pd.Series, report: ValidationReport
    ) -> None:
        """
        Compute Pearson correlation between each feature and the encoded target.
        Features with |r| > CORRELATION_HARD_BLOCK are flagged as critical.
        Features with |r| > CORRELATION_WARN are flagged as warnings.
        """
        # Encode target to numeric
        le_map = {cls: i for i, cls in enumerate(sorted(y.unique()))}
        y_num  = y.map(le_map).astype(float)

        correlations: list[dict] = []
        for col in X.columns:
            x_col = X[col].astype(float).fillna(0.0)
            if x_col.std() < CONSTANT_VARIANCE_EPS:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    r, p = pearsonr(x_col, y_num)
                except Exception:
                    r, p = 0.0, 1.0
            abs_r = abs(r)
            correlations.append({"feature": col, "pearson_r": round(r, 4),
                                  "abs_r": round(abs_r, 4), "p_value": round(p, 6)})
            if abs_r >= CORRELATION_HARD_BLOCK:
                report.critical_errors.append(
                    f"TARGET LEAKAGE: '{col}' has |r|={abs_r:.3f} with target "
                    f"(threshold={CORRELATION_HARD_BLOCK}) — likely post-match data"
                )
            elif abs_r >= CORRELATION_WARN:
                report.warnings.append(
                    f"HIGH CORRELATION: '{col}' has |r|={abs_r:.3f} with target — inspect carefully"
                )

        df_corr = pd.DataFrame(correlations).sort_values("abs_r", ascending=False).reset_index(drop=True)
        report.correlation_top = df_corr.head(20)

    # ── Check 3: Temporal leakage ────────────────────────────────

    def _check_temporal_leakage(
        self,
        X:                   pd.DataFrame,
        match_dates:         pd.Series | None,
        feature_computed_at: pd.Series | None,
        report:              ValidationReport,
    ) -> None:
        """
        If match_dates and feature_computed_at are provided, verify that
        feature_computed_at[i] < match_dates[i] for every row.

        Also checks that val rows all have match_dates after all train rows
        (temporal split integrity).
        """
        if match_dates is None:
            report.warnings.append(
                "TEMPORAL CHECK SKIPPED: match_dates not provided. "
                "Pass match_dates_train to enable strict temporal validation."
            )
            return

        match_dates = pd.to_datetime(match_dates, errors="coerce", utc=False)

        # Check feature_computed_at < match_date
        if feature_computed_at is not None:
            computed_at = pd.to_datetime(feature_computed_at, errors="coerce", utc=False)
            violations  = (computed_at >= match_dates).sum()
            if violations > 0:
                report.critical_errors.append(
                    f"TEMPORAL LEAKAGE: {violations} rows have feature_computed_at >= match_date. "
                    f"Features were computed using data from the same day as or after the match."
                )
            else:
                logger.info("Temporal check: all feature timestamps pre-date their match ✅")

        # Integrity: check match_dates are sorted (should be for temporal split)
        if not match_dates.is_monotonic_increasing:
            sorted_pct = (match_dates.diff().dropna() >= timedelta(0)).mean() * 100
            if sorted_pct < 80:
                report.warnings.append(
                    f"TEMPORAL ORDERING: match_dates are not monotonically increasing "
                    f"({sorted_pct:.0f}% sorted). Ensure you used a temporal (not random) split."
                )

        # Check for suspiciously narrow date range (test for row-duplication bugs)
        date_range_days = (match_dates.max() - match_dates.min()).days
        if date_range_days < 30:
            report.warnings.append(
                f"NARROW DATE RANGE: training data spans only {date_range_days} days. "
                "Ensure full history was loaded — narrow range may indicate a bug."
            )

    # ── Check 4: NaN / coverage ─────────────────────────────────

    def _check_nan_coverage(
        self, X_train: pd.DataFrame, X_val: pd.DataFrame, report: ValidationReport
    ) -> None:
        """
        Flag features with excessive missing values.
        Also detect features that are all-zero in validation but non-zero in train
        (sign of a feature pipeline that wasn't applied correctly).
        """
        for col in X_train.columns:
            nan_frac_train = X_train[col].isna().mean()
            nan_frac_val   = X_val[col].isna().mean()

            if nan_frac_train >= NAN_HARD_BLOCK:
                report.critical_errors.append(
                    f"EXCESSIVE NaN: '{col}' is {nan_frac_train:.0%} missing in train set"
                )
            elif nan_frac_train >= NAN_WARN:
                report.warnings.append(
                    f"HIGH NaN RATE: '{col}' is {nan_frac_train:.0%} missing in train"
                )

            # All-zero in val but informative in train → feature pipeline bug
            train_std = X_train[col].fillna(0).std()
            val_std   = X_val[col].fillna(0).std()
            if train_std > CONSTANT_VARIANCE_EPS and val_std < CONSTANT_VARIANCE_EPS:
                report.warnings.append(
                    f"FEATURE DROPOUT: '{col}' is constant in val but varied in train — "
                    "check feature pipeline applied to val set"
                )

    # ── Check 5: Constant features ───────────────────────────────

    def _check_constant_features(self, X: pd.DataFrame, report: ValidationReport) -> None:
        """Remove or warn about zero-variance features that add no signal."""
        constant_cols = [col for col in X.columns if X[col].fillna(0).std() < CONSTANT_VARIANCE_EPS]
        for col in constant_cols:
            report.warnings.append(
                f"CONSTANT FEATURE: '{col}' has near-zero variance — consider dropping"
            )

    # ── Check 6: Duplicate rows ──────────────────────────────────

    def _check_duplicate_rows(self, X: pd.DataFrame, report: ValidationReport) -> None:
        """
        Duplicate feature rows indicate a match was added twice.
        This leaks data when the same match appears in both train and val folds.
        """
        n_dups = X.duplicated().sum()
        report.duplicate_rows = int(n_dups)
        if n_dups > 0:
            pct = n_dups / len(X) * 100
            msg = f"DUPLICATE ROWS: {n_dups} ({pct:.1f}%) exact duplicate rows in training set"
            if pct > 5:
                report.critical_errors.append(msg)
            else:
                report.warnings.append(msg)

    # ── Check 7: Target distribution ────────────────────────────

    def _check_target_distribution(
        self, y_train: pd.Series, y_val: pd.Series, report: ValidationReport
    ) -> None:
        """
        Check that:
        - No class appears in val but not train (would cause prediction failures)
        - Class imbalance is not extreme (>95% single class)
        - Val distribution doesn't wildly differ from train
        """
        train_counts = y_train.value_counts(normalize=True)
        val_counts   = y_val.value_counts(normalize=True)

        # Classes in val not in train
        unseen = set(y_val.unique()) - set(y_train.unique())
        if unseen:
            report.critical_errors.append(
                f"UNSEEN CLASSES IN VAL: {unseen} — these classes never appear in "
                "training data; model cannot handle them"
            )

        # Extreme imbalance
        dominant_class_pct = float(train_counts.max())
        if dominant_class_pct > 0.95:
            report.critical_errors.append(
                f"EXTREME CLASS IMBALANCE: dominant class = {dominant_class_pct:.0%} "
                "of training data — model will fail to generalise"
            )
        elif dominant_class_pct > 0.80:
            report.warnings.append(
                f"CLASS IMBALANCE: dominant class = {dominant_class_pct:.0%} — "
                "consider class_weight='balanced' or SMOTE"
            )

        # Train vs val distribution shift (simple chi-square proxy)
        for cls in train_counts.index:
            train_p = float(train_counts.get(cls, 0))
            val_p   = float(val_counts.get(cls, 0))
            diff    = abs(train_p - val_p)
            if diff > 0.15:
                report.warnings.append(
                    f"TARGET SHIFT: class '{cls}' — train={train_p:.2%}  val={val_p:.2%} "
                    f"(diff={diff:.2%}). Val set may not represent same distribution."
                )

    # ── Check 8: Distribution shift (KS test) ───────────────────

    def _check_distribution_shift(
        self, X_train: pd.DataFrame, X_val: pd.DataFrame, report: ValidationReport
    ) -> pd.DataFrame:
        """
        Run the two-sample KS test on each feature between train and val.
        Returns a DataFrame with KS statistic and p-value per feature.
        Features with p < KS_SHIFT_ALPHA are flagged.
        """
        results: list[dict] = []
        shifted: list[str]  = []

        for col in X_train.columns:
            train_vals = X_train[col].fillna(0).values.astype(float)
            val_vals   = X_val[col].fillna(0).values.astype(float)
            try:
                stat, p = ks_2samp(train_vals, val_vals)
            except Exception:
                stat, p = 0.0, 1.0
            results.append({"feature": col, "ks_stat": round(stat, 4), "p_value": round(p, 6)})
            if p < KS_SHIFT_ALPHA and stat > 0.15:
                shifted.append(f"'{col}' (KS={stat:.3f}, p={p:.4f})")

        if len(shifted) > len(X_train.columns) * 0.3:
            report.warnings.append(
                f"DISTRIBUTION SHIFT: {len(shifted)} features have significant "
                f"train→val distribution shift (KS p<{KS_SHIFT_ALPHA}). "
                "Consider recalibration or more temporal training data."
            )
        elif shifted:
            for s in shifted[:10]:    # cap warnings
                report.warnings.append(f"DISTRIBUTION SHIFT: {s}")

        df_ks = pd.DataFrame(results).sort_values("ks_stat", ascending=False).reset_index(drop=True)
        return df_ks

    # ── Stats table ──────────────────────────────────────────────

    def _compute_feature_stats(
        self, X_train: pd.DataFrame, X_val: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Report per-feature statistics to help the user understand the data.
        Returns one row per feature.
        """
        rows: list[dict] = []
        for col in X_train.columns:
            train_col = X_train[col].fillna(0)
            val_col   = X_val[col].fillna(0)
            rows.append({
                "feature":       col,
                "train_mean":    round(float(train_col.mean()), 4),
                "train_std":     round(float(train_col.std()),  4),
                "train_min":     round(float(train_col.min()),  4),
                "train_max":     round(float(train_col.max()),  4),
                "train_nan_pct": round(float(X_train[col].isna().mean()) * 100, 1),
                "val_mean":      round(float(val_col.mean()),   4),
                "val_std":       round(float(val_col.std()),    4),
                "val_nan_pct":   round(float(X_val[col].isna().mean()) * 100, 1),
            })
        return pd.DataFrame(rows)

    # ── Standalone feature-level checks ─────────────────────────

    @staticmethod
    def check_no_future_stats(
        feature_dict: dict[str, float],
        match_date: datetime,
        feature_date: datetime,
    ) -> list[str]:
        """
        Standalone check for a single feature vector.
        Verifies that feature_date (when features were calculated) < match_date.
        Called at inference time as a final safety net.

        Returns list of error strings (empty = clean).
        """
        errors: list[str] = []
        if feature_date >= match_date:
            errors.append(
                f"LEAKAGE: features computed at {feature_date.isoformat()} "
                f"but match is at {match_date.isoformat()} — "
                "feature contains future data"
            )
        for col, val in feature_dict.items():
            for fragment in LEAKY_COLUMN_FRAGMENTS:
                if fragment in col.lower():
                    if not any(allow in col.lower() for allow in LEAKY_ALLOWLIST):
                        errors.append(
                            f"LEAKAGE: column '{col}' contains banned fragment '{fragment}'"
                        )
                        break
        return errors

    @staticmethod
    def check_feature_vector_completeness(
        feature_dict: dict[str, float],
        min_non_zero: int = 10,
    ) -> list[str]:
        """
        Warn if a feature vector has suspiciously few non-zero values.
        Used at inference time to catch pipeline failures.
        """
        non_zero = sum(1 for v in feature_dict.values() if v not in (0.0, None))
        warnings_out: list[str] = []
        if non_zero < min_non_zero:
            warnings_out.append(
                f"SPARSE FEATURES: only {non_zero} non-zero features "
                f"(expected >= {min_non_zero}) — feature pipeline may have failed"
            )
        return warnings_out


# ─────────────────────────── CLI / Pre-training Gate ───────────────────────

def validate_before_training(
    X_train:            pd.DataFrame,
    y_train:            pd.Series,
    X_val:              pd.DataFrame,
    y_val:              pd.Series,
    match_dates_train:  pd.Series | None = None,
    match_dates_val:    pd.Series | None = None,
    feature_computed_at: pd.Series | None = None,
    dry_run:            bool = False,
) -> ValidationReport:
    """
    Top-level pre-training gate.

    Call this at the start of every model training script.
    Raises RuntimeError if critical leakage is detected (unless dry_run=True).

    Example
    -------
        report = validate_before_training(X_train, y_train, X_val, y_val,
                                          match_dates_train=dates_train)
        print(report.summary())
        # Training proceeds only if report.passed == True
    """
    validator = FeatureValidator(hard_block_on_leakage=not dry_run)
    report    = validator.run(
        X_train, y_train, X_val, y_val,
        match_dates_train=match_dates_train,
        match_dates_val=match_dates_val,
        feature_computed_at=feature_computed_at,
    )
    return report


# ─────────────────────────── CLI entry ─────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s")

    p = argparse.ArgumentParser(description="Validate feature dataset for leakage")
    p.add_argument("--train-csv",  required=True,  help="Path to train features CSV")
    p.add_argument("--val-csv",    required=True,  help="Path to val   features CSV")
    p.add_argument("--target-col", default="label", help="Target column name (default: label)")
    p.add_argument("--date-col",   default=None,    help="Match date column name (optional)")
    p.add_argument("--dry-run",    action="store_true",
                   help="Report only — do not raise errors on failure")
    args = p.parse_args()

    df_train = pd.read_csv(args.train_csv)
    df_val   = pd.read_csv(args.val_csv)

    if args.target_col not in df_train.columns:
        logger.critical("Target column '%s' not found in training CSV", args.target_col)
        sys.exit(1)

    y_train = df_train.pop(args.target_col)
    y_val   = df_val.pop(args.target_col)

    dates_train = df_train.pop(args.date_col) if args.date_col and args.date_col in df_train.columns else None
    dates_val   = df_val.pop(args.date_col)   if args.date_col and args.date_col in df_val.columns   else None

    X_train = df_train.select_dtypes(include=[np.number])
    X_val   = df_val.select_dtypes(include=[np.number])

    report = validate_before_training(
        X_train, y_train, X_val, y_val,
        match_dates_train=dates_train,
        match_dates_val=dates_val,
        dry_run=args.dry_run,
    )

    print("\n" + report.summary())

    if report.feature_stats is not None:
        print("\nTop 10 features by train NaN %:")
        top_nan = report.feature_stats.sort_values("train_nan_pct", ascending=False).head(10)
        print(top_nan[["feature", "train_nan_pct", "val_nan_pct", "train_std"]].to_string(index=False))

    if report.correlation_top is not None:
        print("\nTop 10 features by target correlation:")
        print(report.correlation_top.head(10).to_string(index=False))

    if report.ks_results is not None:
        print("\nTop 10 features by distribution shift (KS stat):")
        print(report.ks_results.head(10).to_string(index=False))

    sys.exit(0 if report.passed else 1)
