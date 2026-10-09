from collections import Counter, defaultdict


class SignalFunnel:
    def __init__(self):
        self.counts = Counter()
        self.rejections = defaultdict(Counter)
        self.direction_rejections = defaultdict(Counter)
        self.last = {}

    def __call__(self, stage, reason=None, details=None):
        self.counts[stage] += 1
        if reason:
            self.rejections[stage][reason] += 1
        details = details or {}
        for side, reasons in details.get("direction_rejections", {}).items():
            self.direction_rejections[side].update(reasons)
        self.last = {"stage": stage, "reason": reason, "details": details}

    def report(self):
        return {"counts": dict(self.counts), "rejections": {k: dict(v) for k, v in self.rejections.items()},
                "direction_rejections": {k: dict(v) for k, v in self.direction_rejections.items()}}
