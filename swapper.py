from tinyman.v1.client import TinymanMainnetClient

class Swapper:
    """A class to initiate the Tinyman client with a given algo wallet"""
    def __init__(self, address, private_key):
        self.private_key = private_key
        self.address = address
        self.client = TinymanMainnetClient(user_address=address)
        
        # Check if the account is opted into Tinyman and optin if necessary
        if(not self.client.is_opted_in()):
            print('Account not opted into app, opting in now..')
            transaction_group = self.client.prepare_app_optin_transactions()
            transaction_group.sign_with_private_key(self.address, self.private_key)
            result = self.client.submit(transaction_group, wait=True)


    def swap(self, asset1_id, asset2_id, quantity, target_price, slippage=0.01, excess_min=0.01):
        """Swap a given quantity asset1 for asset2 if its value is above target_price."""
        asset1 = self.client.fetch_asset(asset1_id) #ALGO
        asset2 = self.client.fetch_asset(asset2_id) #USDC
        # Fetch the pool we will work with
        pool = self.client.fetch_pool(asset2, asset1)
        # Get a quote for a swap of 1 asset1 to asset2 with given slippage tolerance
        quote = pool.fetch_fixed_input_swap_quote(asset1(quantity), slippage=slippage)
        print(quote)
        print(f'{asset2.name} per {asset1.name}: {quote.price} (worst case: {quote.price_with_slippage})')
        
        # We only want to sell if asset1 is > target_price*asset2
        if quote.price_with_slippage > target_price:
            print(f'Swapping {quote.amount_in} to {quote.amount_out_with_slippage}')
            # Prepare a transaction group
            transaction_group = pool.prepare_swap_transactions_from_quote(quote)
            # Sign the group with our key
            transaction_group.sign_with_private_key(self.address, self.private_key)
            # Submit transactions to the network and wait for confirmation
            result = self.client.submit(transaction_group, wait=True)

            # Check if any excess remaining after the swap
            excess = pool.fetch_excess_amounts()
            if asset2 in excess:
                amount = excess[asset2]
                print(f'Excess: {amount}')
                # We might just let the excess accumulate rather than redeeming if its < excess_min*asset2
                if amount > excess_min:
                    transaction_group = pool.prepare_redeem_transactions(amount)
                    transaction_group.sign_with_private_key(self.address, self.private_key)
                    result = self.client.submit(transaction_group, wait=True)
