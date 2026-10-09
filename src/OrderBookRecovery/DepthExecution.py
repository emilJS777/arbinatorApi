import math


def consume_book(book, side, base_amount, fee_percent=0):
    """All-or-none market fill at observed depth; never invent missing liquidity."""
    if not math.isfinite(base_amount) or base_amount <= 0:
        return None
    rows = book.get("asks" if side in {"long", "buy"} else "bids") or []
    levels = []
    for row in rows:
        price, amount = (row["price"], row["amount"]) if isinstance(row, dict) else row[:2]
        price, amount = float(price), float(amount)
        if not math.isfinite(price) or not math.isfinite(amount) or price <= 0 or amount < 0:
            return None
        levels.append((price, amount))
    levels.sort(reverse=side not in {"long", "buy"})
    remaining, cost = base_amount, 0.0
    for price, amount in levels:
        quantity = min(remaining, amount)
        remaining -= quantity
        cost += quantity * price
        if remaining <= base_amount * 1e-12:
            return {"price": cost / base_amount, "fee": cost * fee_percent / 100,
                    "amount": base_amount, "cost": cost}
    return None
