# Most liquid assets
assets = [163650, 283820866, 31566704, 226701642, 320259224, 27165954,
          312769, 300208676, 384303832, 287867876, 230946361,
          137594422, 310014962, 378382099, 137020565, 297995609,
          241759159]


def get_name(asset_id):
    """Return the name and ticker for a given asset id"""
    return {
        0: ('Algorand', 'ALGO'),
        163650: ('Asia Reserve Currency Coin', 'ARCC'),
        283820866: ('Xfinite Entertainment Token', 'XET'),
        31566704: ('USDC', 'USDC'),
        226701642: ('Yieldly', 'YLDY'),
        320259224: ('Wrapped Algo', 'wALGO'),
        27165954: ('PLANET', 'Planets'),
        312769: ('Tether USDt', 'USDt'),
        300208676: ('Smile Coin', 'SMILE'),
        384303832: ('AKITA INU TOKEN', 'AKITA'),
        287867876: ('Opulous', 'OPUL'),
        230946361: ('AlgoGems', 'GEMS'),
        137594422: ('HEADLINE', 'HDL'),
        310014962: ('AlcheCoin', 'ALCH'),
        378382099: ('Tinychart Token', 'TINY'),
        137020565: ('Buy Token', 'BUY'),
        297995609: ('Choice Coin', 'Choice'),
        241759159: ('Freckle', 'FRKL'),
    }.get(asset_id, (None, None))
