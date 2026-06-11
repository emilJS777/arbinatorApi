from decimal import Decimal, InvalidOperation


class OrderBookNormalizer:
    @staticmethod
    def normalize(order_book: dict) -> tuple[dict | None, str | None]:
        if not isinstance(order_book, dict):
            return None, "unsupported_order_book_format"

        bid_rows = OrderBookNormalizer.first_present(order_book, ["bids", "purchases", "buy", "buys"])
        ask_rows = OrderBookNormalizer.first_present(order_book, ["asks", "sales", "sell", "sells"])

        if bid_rows is None and ask_rows is None:
            return None, "unsupported_order_book_format"
        if not bid_rows:
            return None, "empty_bids"
        if not ask_rows:
            return None, "empty_asks"

        bids = OrderBookNormalizer.normalize_side(bid_rows)
        asks = OrderBookNormalizer.normalize_side(ask_rows)
        if bids is None or asks is None:
            return None, "invalid_price_amount"
        if not bids:
            return None, "empty_bids"
        if not asks:
            return None, "empty_asks"
        return {"bids": bids, "asks": asks}, None

    @staticmethod
    def normalize_side(rows):
        result = []
        for row in rows:
            price, amount = OrderBookNormalizer.extract_price_amount(row)
            if price is None or amount is None or price <= 0 or amount < 0:
                return None
            result.append({"price": price, "amount": amount})
        return result

    @staticmethod
    def first_present(order_book, keys):
        for key in keys:
            if key in order_book:
                return order_book.get(key)
        return None

    @staticmethod
    def extract_price_amount(row):
        if isinstance(row, dict):
            price = row.get("price")
            amount = row.get("amount")
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price = row[0]
            amount = row[1]
        else:
            return None, None

        try:
            return Decimal(str(price)), Decimal(str(amount))
        except (InvalidOperation, TypeError, ValueError):
            return None, None
