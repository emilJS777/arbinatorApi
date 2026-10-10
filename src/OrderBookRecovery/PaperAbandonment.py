def abandonment_mode_error(trade, parse_json):
    if trade.execution_mode != "paper":
        return "abandon_requires_confirmed_paper_mode"
    # These are exchange evidence, unlike live_entry_fee which paper fills also use.
    fields = ("live_exchange_order_id", "live_close_order_id", "live_entry_price",
              "live_exit_price", "live_exit_fee", "live_filled_amount", "live_client_order_id",
              "live_close_client_order_id", "live_raw_open_response_json",
              "live_raw_close_response_json", "exchange_tp_order_id", "exchange_sl_order_id",
              "exchange_tp_price", "exchange_sl_price", "tp_sl_created_at",
              "protection_checked_at", "funding_checked_at")
    if any(getattr(trade, field, None) is not None for field in fields):
        return "abandon_rejected_live_execution_evidence"
    if trade.tp_sl_protected or trade.live_status not in (None, "", "paper_abandoned"):
        return "abandon_rejected_live_or_pending_execution_evidence"
    if trade.pnl_source in ("exchange_realized_pnl", "order_history", "verified_fills_with_funding",
                           "exchange_order_details_excluding_funding", "verified_fills_funding_pending"):
        return "abandon_rejected_live_execution_evidence"
    for raw in (trade.execution_config_json, trade.decision_snapshot_json):
        parsed = parse_json(raw)
        if isinstance(parsed, dict):
            nested = parsed.get("config")
            if parsed.get("execution_mode") == "live" or (isinstance(nested, dict) and nested.get("execution_mode") == "live"):
                return "abandon_rejected_live_execution_evidence"
    return None
