"""Executable quantity from contract lots; never round risk upwards to reach minimums."""
from decimal import Decimal, ROUND_DOWN
import math


def executable_amount(requested, price, metadata, notional_cap=None, strict=False):
    limits, precision = metadata.get("limits"), metadata.get("precision")
    size = metadata.get("contract_size")
    if not limits or not precision or not size or metadata.get("precision_mode", 4) != 4:
        return (None, "contract_metadata_missing") if strict else (requested, None)
    step = precision.get("amount")
    if step is None or not math.isfinite(float(step)) or float(step) <= 0 or float(size) <= 0:
        return None, "contract_precision_missing"
    step_base = Decimal(str(step)) * Decimal(str(size))
    amount = float((Decimal(str(requested)) / step_base).to_integral_value(rounding=ROUND_DOWN) * step_base)
    quantity = amount / float(size)
    amount_limits = limits.get("amount") or {}
    cost_limits = limits.get("cost") or {}
    if strict and amount_limits.get("min") is None:
        return None, "contract_min_amount_missing"
    if amount <= 0 or quantity < float(amount_limits.get("min") or 0):
        return None, "below_contract_min_amount"
    if amount_limits.get("max") and quantity > float(amount_limits["max"]):
        return None, "above_contract_max_amount"
    cost = amount * price
    if cost < float(cost_limits.get("min") or 0):
        return None, "below_contract_min_notional"
    if notional_cap is not None and cost > notional_cap * (1 + 1e-10):
        return None, "executable_notional_exceeds_risk_cap"
    return amount, None
