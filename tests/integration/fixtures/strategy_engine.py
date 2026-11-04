from EC_API.common.data_feeds import DataFeed

class StrategyEngine:
    def __init__(self):
        self.datafeeds: dict[str, DataFeed] = dict()
        ...