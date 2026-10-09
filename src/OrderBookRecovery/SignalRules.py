def direction_rejections(config, consensus):
    result = {}
    for side, sign in (("long", 1), ("short", -1)):
        reasons = []
        if consensus.get("valid_exchanges_count", 0) < config.min_valid_exchanges:
            reasons.append("not_enough_valid_exchanges")
        if not consensus.get("configured_exchange_valid"):
            reasons.append(consensus.get("configured_exchange_reject_reason") or "configured_exchange_invalid")
        if consensus.get(f"confirming_{side}_count", 0) < config.min_confirming_exchanges:
            reasons.append("confirming_count_below_min")
        if consensus.get(f"consensus_ratio_{side}", 0) < config.min_consensus_ratio:
            reasons.append("consensus_ratio_below_min")
        if (consensus.get("average_momentum") or 0) * sign <= 0:
            reasons.append("momentum_wrong_sign_or_zero")
        median = consensus.get("median_imbalance")
        if config.use_median_imbalance and (median is None or (median <= config.long_imbalance_threshold if side == "long" else median > config.short_imbalance_threshold)):
            reasons.append("median_imbalance_threshold_not_met")
        if config.require_configured_exchange_signal and not consensus.get(f"configured_exchange_{side}_signal"):
            reasons.append("configured_exchange_direction_not_confirmed")
        result[side] = reasons
    return result


def consensus_side(config, consensus):
    """Shared deterministic signal rules for forward trading and recorded-book replay."""
    if consensus.get("valid_exchanges_count", 0) < config.min_valid_exchanges:
        return None, "not_enough_valid_exchanges"
    if not consensus.get("configured_exchange_valid"):
        return None, consensus.get("configured_exchange_reject_reason") or "configured_exchange_snapshot_missing_or_invalid"
    median = consensus.get("median_imbalance")
    if config.use_median_imbalance and median is None:
        return None, "no_valid_median_imbalance"
    momentum = consensus.get("average_momentum") or 0
    for side, direction in (("long", 1), ("short", -1)):
        if consensus.get(f"confirming_{side}_count", 0) < config.min_confirming_exchanges:
            continue
        if consensus.get(f"consensus_ratio_{side}", 0) < config.min_consensus_ratio:
            continue
        if momentum * direction <= 0:
            continue
        if config.use_median_imbalance:
            if side == "long" and median <= config.long_imbalance_threshold:
                continue
            if side == "short" and median > config.short_imbalance_threshold:
                continue
        if config.require_configured_exchange_signal and not consensus.get(f"configured_exchange_{side}_signal"):
            continue
        return side, None
    return None, "no_consensus"
