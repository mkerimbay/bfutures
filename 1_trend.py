from helper import update_trend, adjust_leverages, update_denom

"""
 - Detect trends, to be run 23:55 UTC
 - Adjust leverages on tradable symbols to user defined leverage
"""


if __name__ == '__main__':
    update_trend()
    adjust_leverages()
    update_denom()
