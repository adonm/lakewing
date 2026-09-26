"""OSB's standard range randomizer supplies fresh time windows per request."""

import os
import random

TS_BASE = 1727240000


def register(registry):
    docs = int(os.environ["OSB_DOCS"])
    width = max(1, int(docs * float(os.environ["OSB_WINDOW_FRAC"])))

    def window():
        start = TS_BASE + random.randrange(max(1, docs - width))
        return {"gte": start, "lte": start + width}

    for clients in (1, 8):
        registry.register_standard_value_source(
            f"fresh-window-{clients}", "timestamp_nanos", window
        )
