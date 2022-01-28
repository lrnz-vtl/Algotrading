# Algotrading
Algorand trading on tinyman

## Installation

To run this code requires:
- tinyman.py.sdk https://github.com/tinymanorg/tinyman-py-sdk
- py-algorand-sdk https://py-algorand-sdk.readthedocs.io

These packages could be installed separately as
```
pip install git+https://github.com/tinymanorg/tinyman-py-sdk.git
pip install py-algorand-sdk
```

For the complete installation, the python version is 3.10.0. 
The easiest is to install miniconda, create am environment and install the packages:
```
conda create --name Algotrading python=3.10.0
conda activate Algotrading
pip install -r requirements.txt
```



## Conceptual framework

Suppose we have 3 coins c0, c1, c2, and we have a separate forecast for each of the tree pools i.e. what the relative price of c0 with respect to c1 will be in the future. c0 is our reference coin (which we can easily redeem for dollars).

We denote the 3 forecasts as f01, f12, f20, which e.g. represent in percentage how much the relative prices will change compared to the relative prices now. 
Suppose also that in this example we have f01 = f20 = 0, while f12 > 0 , which means that c1 will increase in value w.r.t. c2 in the future, while all other relative prices will remain unchanged. How do we use this information?

In that situation, in the future, there will be a "triangle imbalance" that a high frequency arbitrageur might exploit. How do we use the fact that we know this information in advance? Because we can load on c1 *now*, and later we can offload it via the transaction c1 -> c2 -> c0 , therefore performing only two legs of the transaction in a short time. This has the advantage that we save on transaction cost and avoids some of the latency risk. (It would be nice if we could short coin 2 in advance instead, to avoid most of the latency risk, but we can't do that). 

So the components we need for the framework to work are the following:
- Forecasts for all the pairs (relative prices)
- A pricing function that tells us the best estimate for the future value of any  position (this takes into account all the forecasts, and also should accurately estimate all the future transaction costs of offloading all the other coins to c0 (by finding the "least cost" paths through the graphs))
- An optimisation engine that finds the most valuable position we can achieve (take into account forecasts etc.)
- An execution engine which realises that position. Perhaps this should also exploit any immediate arbitrages whenever they are present, because they are probably very short-lived.
python==3.10.0
arrow==1.2.1
asyncio==3.4.3
matplotlib==3.5.1
numpy==1.21.5
pandas==1.3.5
py-algorand-sdk==1.8.0
python-dateutil==2.8.2
pytz==2021.3
requests==2.26.0
tinyman-py-sdk @ git+https://github.com/tinymanorg/tinyman-py-sdk.git@d36a9c1b5f8f69a8a24b780fdab3e0afb1d76d72
urllib3==1.26.7
