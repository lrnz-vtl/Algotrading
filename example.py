from swapper import Swapper
from tinydata import TinyData
import pprint, algosdk

#address='CO5Y23AD.......'
#private_key=algosdk.mnemonic.to_private_key('list of 25 words from wallet')

# swapper = Swapper(address, private_key)

# swap some Algorand (ID=0) for USDC (ID=31566704) if algo > 1.7$
# note that quantity is expressed in millionth
asset1_id=0
asset2_id=31566704
# swapper.swap(asset1_id, asset2_id, 1_000_000, 1.7)

data=TinyData()
print('USDC:')
pprint.pprint(data.asset_summary(asset2_id))
