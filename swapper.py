from tinyman.v1.client import TinymanMainnetClient, TinymanTestnetClient
from tinyman.v1.optin import prepare_asset_optin_transactions
from tools.timestamp import Timestamp
from trade_logger.base import TradeLogger, TradeLog, TradeInfo
from logging import Logger


class Swapper:
    """A class to initiate the Tinyman client with a given algo wallet"""

    def __init__(self, address, private_key,
                 tradeLogger: TradeLogger,
                 logger: Logger,
                 testnet: bool):
        self.logger = logger
        self.tradeLogger = tradeLogger
        self.private_key = private_key
        self.address = address

        if testnet:
            client = TinymanTestnetClient
        else:
            client = TinymanMainnetClient
        self.client = client(user_address=address)

        # Check if the account is opted into Tinyman and optin if necessary
        if not self.client.is_opted_in():
            self.logger.info('Account not opted into app, opting in now..')
            transaction_group = self.client.prepare_app_optin_transactions()
            transaction_group.sign_with_private_key(self.address, self.private_key)
            result = self.client.submit(transaction_group, wait=True)

    def asset_optin(self, asset_id):
        """Opt in to an asset"""
        # for algorand, nothing to be done
        if (asset_id==0):
            return
        acc_info = self.client.algod.account_info(self.address)
        optin = False
        for a in acc_info['assets']:
            if a['asset-id']==asset_id:
                optin = True
                break
        if (not optin):
            txn_group = prepare_asset_optin_transactions(
                asset_id =asset_id,
                sender=self.address,
                suggested_params=self.client.algod.suggested_params()
            )
            txn_group.sign_with_private_key(self.address, self.private_key)
            result = self.client.submit(txn_group, wait=True)


    def swap(self, asset1_id, asset2_id, quantity, target_price,
             slippage=0.01, excess_min=0.01, skip_optin=False):
        """Swap a given quantity asset1 for asset2 if its value is above target_price."""

        self.logger.info(f'Attempting to trade {asset2_id} per {asset1_id}')

        asset1 = self.client.fetch_asset(asset1_id)  # ALGO
        asset2 = self.client.fetch_asset(asset2_id)  # USDC
        # check if assets are authorized on wallet and add them if they are not
        if (not skip_optin):
            self.asset_optin(asset1_id)
            self.asset_optin(asset2_id)

        # Fetch the pool we will work with
        pool = self.client.fetch_pool(asset2, asset1)
        # Get a quote for a swap of 1 asset1 to asset2 with given slippage tolerance
        quote = pool.fetch_fixed_input_swap_quote(asset1(quantity), slippage=slippage)

        self.logger.info(f"Got quote: {quote}")
        self.logger.info(f"quote.price_with_slippage = {quote.price_with_slippage}")

        # We only want to sell if asset1 is > target_price*asset2
        if quote.price_with_slippage > target_price:
            self.logger.info(f'Swapping {quote.amount_in} to {quote.amount_out_with_slippage}')
            # Prepare a transaction group
            transaction_group = pool.prepare_swap_transactions_from_quote(quote)
            # Sign the group with our key
            transaction_group.sign_with_private_key(self.address, self.private_key)
            # Submit transactions to the network and wait for confirmation
            result = self.client.submit(transaction_group, wait=True)

            tradeInfo = TradeInfo(asset1_id=asset1_id,
                                  asset2_id=asset2_id,
                                  quantity=quantity,
                                  target_price=target_price,
                                  slippage=slippage,
                                  excess_min=excess_min,
                                  quote=quote
                                  )

            self.log_trade(tradeInfo=tradeInfo)

            # Check if any excess remaining after the swap
            excess = pool.fetch_excess_amounts()
            if asset2 in excess:
                amount = excess[asset2]
                self.logger.info(f'Excess: {amount}')
                # We might just let the excess accumulate rather than redeeming if its < excess_min*asset2
                if amount > excess_min:
                    transaction_group = pool.prepare_redeem_transactions(amount)
                    transaction_group.sign_with_private_key(self.address, self.private_key)
                    result = self.client.submit(transaction_group, wait=True)

        else:
            self.logger.info("Trade canceled because quote.price_with_slippage <= target_price")

    def log_trade(self, tradeInfo: TradeInfo):
        self.tradeLogger.log(TradeLog(tradeInfo=tradeInfo, timestamp=Timestamp.get()))
