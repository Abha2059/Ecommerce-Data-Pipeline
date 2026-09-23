"""
Data Quality Validation and Quarantine Engine
Performs pre-transformation and post-transformation data quality checks,
quarantines anomalous records with detailed failure reasons, and produces audit reports.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("validation.data_quality")


class CriticalDataQualityError(Exception):
    """Raised when critical schema integrity or quarantine thresholds fail."""
    pass


@dataclass
class RuleEvaluation:
    rule_name: str
    status: str  # "PASSED" or "FAILED"
    records_evaluated: int
    records_failed: int
    error_message: str


@dataclass
class ValidationReport:
    total_records: int
    valid_records: int
    rejected_records: int
    quarantine_pct: float
    execution_timestamp: str
    status: str  # "PASSED", "WARNING", "FAILED"
    evaluations: List[RuleEvaluation] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DataQualityValidator:
    """Validates raw and transformed sales records, routing errors to quarantine."""

    REQUIRED_COLUMNS = [
        "order_id", "order_date", "customer_id", "product_id", 
        "quantity", "unit_price"
    ]

    def __init__(
        self,
        quarantine_dir: Optional[Path] = None,
        max_quarantine_pct: Optional[float] = None
    ):
        self.quarantine_dir = Path(quarantine_dir or cfg.paths.quarantine_data_dir)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.max_quarantine_pct = max_quarantine_pct or cfg.validation.max_quarantine_pct

    def validate_schema(self, df: pd.DataFrame) -> None:
        """Verifies that mandatory columns exist in DataFrame. Fails critically if missing."""
        missing = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            err = f"CRITICAL: Schema validation failed. Missing mandatory columns: {missing}"
            logger.error(err)
            raise CriticalDataQualityError(err)
        logger.info("Schema integrity verified: All mandatory columns are present")

    def validate_and_quarantine(
        self,
        df: pd.DataFrame,
        valid_customer_ids: Optional[Set[str]] = None,
        valid_product_ids: Optional[Set[str]] = None,
        batch_id: Optional[str] = None
    ) -> Tuple[pd.DataFrame, ValidationReport]:
        """
        Evaluates record-level data quality rules, isolates invalid rows, and saves quarantine logs.
        """
        self.validate_schema(df)
        total_count = len(df)
        if total_count == 0:
            logger.warning("Empty DataFrame provided for validation")
            report = ValidationReport(
                total_records=0,
                valid_records=0,
                rejected_records=0,
                quarantine_pct=0.0,
                execution_timestamp=datetime.now(timezone.utc).isoformat(),
                status="PASSED"
            )
            return df, report

        evaluations: List[RuleEvaluation] = []
        df_working = df.copy()
        
        # Track reasons per row (list of reasons)
        rejection_reasons = [[] for _ in range(total_count)]

        # Rule 1: Null or empty checks for mandatory fields
        for col in ["order_id", "order_date", "customer_id", "product_id"]:
            mask_null = df_working[col].isna() | (df_working[col].astype(str).str.strip() == "")
            null_count = int(mask_null.sum())
            if null_count > 0:
                for idx in df_working[mask_null].index:
                    rejection_reasons[idx].append(f"Missing mandatory field '{col}'")
            evaluations.append(RuleEvaluation(
                rule_name=f"null_check_{col}",
                status="FAILED" if null_count > 0 else "PASSED",
                records_evaluated=total_count,
                records_failed=null_count,
                error_message=f"Found {null_count} null or empty values in '{col}'" if null_count > 0 else "Clean"
            ))

        # Rule 2: Quantity validation (must be positive integer > 0)
        def is_invalid_qty(val: Any) -> bool:
            try:
                q = int(float(val))
                return q <= 0
            except (ValueError, TypeError):
                return True

        qty_invalid_mask = df_working["quantity"].apply(is_invalid_qty)
        qty_fail_count = int(qty_invalid_mask.sum())
        if qty_fail_count > 0:
            for idx in df_working[qty_invalid_mask].index:
                rejection_reasons[idx].append(f"Invalid quantity '{df_working.at[idx, 'quantity']}': must be integer > 0")
        evaluations.append(RuleEvaluation(
            rule_name="valid_quantity_positive",
            status="FAILED" if qty_fail_count > 0 else "PASSED",
            records_evaluated=total_count,
            records_failed=qty_fail_count,
            error_message=f"Found {qty_fail_count} non-positive or non-numeric quantities" if qty_fail_count > 0 else "Clean"
        ))

        # Rule 3: Unit Price validation (must be numeric >= 0)
        def is_invalid_price(val: Any) -> bool:
            try:
                p = float(val)
                return p < 0.0
            except (ValueError, TypeError):
                return True

        price_invalid_mask = df_working["unit_price"].apply(is_invalid_price)
        price_fail_count = int(price_invalid_mask.sum())
        if price_fail_count > 0:
            for idx in df_working[price_invalid_mask].index:
                rejection_reasons[idx].append(f"Invalid unit price '{df_working.at[idx, 'unit_price']}': must be float >= 0")
        evaluations.append(RuleEvaluation(
            rule_name="valid_price_non_negative",
            status="FAILED" if price_fail_count > 0 else "PASSED",
            records_evaluated=total_count,
            records_failed=price_fail_count,
            error_message=f"Found {price_fail_count} negative or non-numeric unit prices" if price_fail_count > 0 else "Clean"
        ))

        # Rule 4: Date Format validation (must parse to valid date)
        def is_invalid_date(val: Any) -> bool:
            if not val or pd.isna(val):
                return True
            try:
                # Accept standard separators
                cleaned = str(val).strip().replace("/", "-")
                parsed = datetime.strptime(cleaned, "%Y-%m-%d")
                return parsed.year < 2000 or parsed.year > 2100
            except ValueError:
                return True

        date_invalid_mask = df_working["order_date"].apply(is_invalid_date)
        date_fail_count = int(date_invalid_mask.sum())
        if date_fail_count > 0:
            for idx in df_working[date_invalid_mask].index:
                rejection_reasons[idx].append(f"Unparseable or out-of-bounds order_date '{df_working.at[idx, 'order_date']}'")
        evaluations.append(RuleEvaluation(
            rule_name="valid_date_format",
            status="FAILED" if date_fail_count > 0 else "PASSED",
            records_evaluated=total_count,
            records_failed=date_fail_count,
            error_message=f"Found {date_fail_count} unparseable dates" if date_fail_count > 0 else "Clean"
        ))

        # Rule 5: Foreign key consistency for customer_id (if reference list provided)
        if valid_customer_ids:
            cust_invalid_mask = ~df_working["customer_id"].astype(str).str.strip().isin(valid_customer_ids) & ~df_working["customer_id"].isna()
            cust_fail_count = int(cust_invalid_mask.sum())
            if cust_fail_count > 0:
                for idx in df_working[cust_invalid_mask].index:
                    rejection_reasons[idx].append(f"Unknown customer_id '{df_working.at[idx, 'customer_id']}'")
            evaluations.append(RuleEvaluation(
                rule_name="fk_customer_id_exists",
                status="FAILED" if cust_fail_count > 0 else "PASSED",
                records_evaluated=total_count,
                records_failed=cust_fail_count,
                error_message=f"Found {cust_fail_count} orphan customer IDs" if cust_fail_count > 0 else "Clean"
            ))

        # Rule 6: Foreign key consistency for product_id (if reference list provided)
        if valid_product_ids:
            prod_invalid_mask = ~df_working["product_id"].astype(str).str.strip().isin(valid_product_ids) & ~df_working["product_id"].isna()
            prod_fail_count = int(prod_invalid_mask.sum())
            if prod_fail_count > 0:
                for idx in df_working[prod_invalid_mask].index:
                    rejection_reasons[idx].append(f"Unknown product_id '{df_working.at[idx, 'product_id']}'")
            evaluations.append(RuleEvaluation(
                rule_name="fk_product_id_exists",
                status="FAILED" if prod_fail_count > 0 else "PASSED",
                records_evaluated=total_count,
                records_failed=prod_fail_count,
                error_message=f"Found {prod_fail_count} orphan product IDs" if prod_fail_count > 0 else "Clean"
            ))

        # Separate valid from quarantined
        is_quarantined = [len(reasons) > 0 for reasons in rejection_reasons]
        quarantine_reasons_str = ["; ".join(reasons) if reasons else None for reasons in rejection_reasons]

        df_working["quarantine_reason"] = quarantine_reasons_str
        df_working["quarantined_at"] = datetime.now(timezone.utc).isoformat()

        valid_df = df_working[~pd.Series(is_quarantined)].drop(columns=["quarantine_reason", "quarantined_at"]).copy()
        quarantine_df = df_working[pd.Series(is_quarantined)].copy()

        rejected_count = len(quarantine_df)
        valid_count = len(valid_df)
        quarantine_pct = round((rejected_count / total_count) * 100.0, 2)

        # Write quarantined records to file
        if rejected_count > 0:
            batch_prefix = batch_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            quarantine_file = self.quarantine_dir / f"quarantine_{batch_prefix}.json"
            quarantine_df.to_json(quarantine_file, orient="records", indent=2)
            logger.warning(
                f"Quarantined {rejected_count}/{total_count} records ({quarantine_pct}%). "
                f"Wrote audit file to: {quarantine_file}"
            )

        status = "PASSED"
        if rejected_count > 0:
            status = "WARNING" if quarantine_pct <= self.max_quarantine_pct else "FAILED"

        report = ValidationReport(
            total_records=total_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            quarantine_pct=quarantine_pct,
            execution_timestamp=datetime.now(timezone.utc).isoformat(),
            status=status,
            evaluations=evaluations
        )

        # Save validation report
        report_file = self.quarantine_dir / f"report_{batch_id or datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)

        # Check threshold
        if quarantine_pct > self.max_quarantine_pct:
            raise CriticalDataQualityError(
                f"Quarantine rate of {quarantine_pct}% exceeded safety ceiling of {self.max_quarantine_pct}%! "
                f"Rejected {rejected_count} out of {total_count} records."
            )

        return valid_df, report
