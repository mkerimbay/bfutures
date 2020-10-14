from helper import store_trades
"""
Daily backup routines, cron: 00:02
- store recent trade history
- backup database
- backup logs
"""
if __name__ == '__main__':
    store_trades()
