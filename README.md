# Algotrading
Algorand trading on tinyman

## Installation

To run this code requires:
- tinyman.py.sdk https://github.com/tinymanorg/tinyman-py-sdk
- ts_tools_algo git+ssh://git@github.com/lrnz-vtl/ts_tools_algo.git
- py-algorand-sdk https://py-algorand-sdk.readthedocs.io

These packages could be installed separately as
```
pip install git+https://github.com/tinymanorg/tinyman-py-sdk.git
pip install git+ssh://git@github.com/lrnz-vtl/ts_tools_algo.git
pip install py-algorand-sdk
```

For the complete installation, the python version is 3.10.0. 
The easiest is to install miniconda, create am environment and install the packages:
```
conda create --name Algotrading python=3.10.0
conda activate Algotrading
pip install -r requirements.txt
```
