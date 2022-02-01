from __future__ import annotations
import json
import logging
import os.path
from pathlib import Path
from algo.universe.assets import CandidateASAStore, AssetType
import argparse
import time

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Fetch and serialise candidate assets.')
    parser.add_argument('-f', dest='base_folder', type=str, required=True)

    args = parser.parse_args()

    timestr = time.strftime("%Y%m%d-%H%M%S")
    base_folder = Path(args.base_folder) / timestr
    os.makedirs(base_folder, exist_ok=True)

    liq_fname = base_folder / f'liquidity_assets.json'
    nliq_fname = base_folder / f'non_liquidity_assets.json'

    all_assets = CandidateASAStore.from_scratch(verified_only=True)

    # FIXME Not sure why this comes up empty
    liquidity_assets = CandidateASAStore.from_store(all_assets, filter_type=AssetType.LIQUIDITY)
    assert all(x.is_liquidity_token for x in liquidity_assets.assets)
    with open(liq_fname, 'w') as f:
        json.dump(liquidity_assets.as_dict(), f)

    not_liquidity_assets = CandidateASAStore.from_store(all_assets, filter_type=AssetType.NOT_LIQUIDITY)
    assert all((not x.is_liquidity_token) for x in not_liquidity_assets.assets)
    with open(nliq_fname, 'w') as f:
        json.dump(not_liquidity_assets.as_dict(), f)



