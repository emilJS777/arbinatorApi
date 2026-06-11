class IndicatorEngine:
    def ema(self, values: list[float], period: int) -> list[float | None]:
        if not values:
            return []
        multiplier = 2 / (period + 1)
        result = []
        ema_value = None
        for index, value in enumerate(values):
            if index + 1 < period:
                result.append(None)
                continue
            if ema_value is None:
                ema_value = sum(values[index + 1 - period:index + 1]) / period
            else:
                ema_value = (value - ema_value) * multiplier + ema_value
            result.append(ema_value)
        return result

    def rsi(self, values: list[float], period: int = 14) -> list[float | None]:
        if len(values) <= period:
            return [None] * len(values)
        result = [None] * len(values)
        gains = []
        losses = []
        for index in range(1, period + 1):
            change = values[index] - values[index - 1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        result[period] = 100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
        for index in range(period + 1, len(values)):
            change = values[index] - values[index - 1]
            gain = max(change, 0)
            loss = abs(min(change, 0))
            avg_gain = ((avg_gain * (period - 1)) + gain) / period
            avg_loss = ((avg_loss * (period - 1)) + loss) / period
            result[index] = 100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
        return result

    def atr(self, candles: list, period: int = 14) -> list[float | None]:
        if not candles:
            return []
        true_ranges = []
        for index, candle in enumerate(candles):
            if index == 0:
                true_ranges.append(candle.high - candle.low)
                continue
            previous_close = candles[index - 1].close
            true_ranges.append(max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            ))
        result = []
        atr_value = None
        for index, value in enumerate(true_ranges):
            if index + 1 < period:
                result.append(None)
                continue
            if atr_value is None:
                atr_value = sum(true_ranges[index + 1 - period:index + 1]) / period
            else:
                atr_value = ((atr_value * (period - 1)) + value) / period
            result.append(atr_value)
        return result

    def latest(self, candles: list) -> dict:
        closes = [candle.close for candle in candles]
        return {
            "ema20": self.ema(closes, 20)[-1] if len(closes) >= 20 else None,
            "ema50": self.ema(closes, 50)[-1] if len(closes) >= 50 else None,
            "rsi14": self.rsi(closes, 14)[-1] if len(closes) >= 15 else None,
            "atr14": self.atr(candles, 14)[-1] if len(candles) >= 14 else None,
        }
