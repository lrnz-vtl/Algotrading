from algo.universe.pools import PoolInfo, PoolInfoStore
import json
import dataclasses


class Universe:
    """ This looks redundant with PoolInfoStore for now but it can be flexibly changed """

    def __init__(self, pools: list[PoolInfo]):
        self.pools = pools

    @staticmethod
    def from_store(ps: PoolInfoStore, n_most_liquid: int):
        def key(p: PoolInfo):
            return -(p.current_asset_1_reserves_in_usd + p.current_asset_2_reserves_in_usd)/2.0
        pools = list(sorted(ps.pools, key=key))[:n_most_liquid]
        return Universe(pools)

    @staticmethod
    def from_cache(fname: str):
        with open(fname) as f:
            data = json.load(f)
        pools = [PoolInfo.from_query_result(x) for x in data]
        return Universe(pools)

    def asdicts(self):
        return [dataclasses.asdict(x) for x in self.pools]