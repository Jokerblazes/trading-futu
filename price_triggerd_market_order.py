from futu import *
import time
import threading

class PriceTriggeredTrader:
    def __init__(self, api_svr_ip='127.0.0.1', api_svr_port=11111, unlock_password=""):
        self.quote_ctx = OpenQuoteContext(host=api_svr_ip, port=api_svr_port)
        self.trade_ctx = OpenSecTradeContext(host=api_svr_ip, port=api_svr_port)
        
        if unlock_password:
            ret, data = self.trade_ctx.unlock_trade(unlock_password)
            if ret != RET_OK:
                print(f"解锁交易失败: {data}")
                return
        
    def set_price_trigger(self, code, buy_price, qty, take_profit_price, stop_loss_price):
        self.buy_order(code, buy_price, qty)
        self.sell_order_with_stop_loss(code, take_profit_price, stop_loss_price, qty)

    def buy_order(self, code, target_price, qty):
        ret, data = self.quote_ctx.subscribe(code, [SubType.QUOTE])
        if ret != RET_OK:
            print(f"订阅行情失败: {data}")
            return
        
        while True:
            ret, data = self.quote_ctx.get_market_snapshot([code])
            if ret != RET_OK:
                print(f"获取快照失败: {data}")
                continue
            
            current_price = data['last_price'][0]
            print(f"当前价格: {current_price}, 目标买入价格: {target_price}")
            
            if current_price <= target_price:
                self.place_market_order(code, qty, TrdSide.BUY)
                break
            
            time.sleep(1)  # 每秒检查一次价格

    def sell_order_with_stop_loss(self, code, take_profit_price, stop_loss_price, qty):
        while True:
            ret, data = self.quote_ctx.get_market_snapshot([code])
            if ret != RET_OK:
                print(f"获取快照失败: {data}")
                continue
            
            current_price = data['last_price'][0]
            print(f"当前价格: {current_price}, 止盈价: {take_profit_price}, 止损价: {stop_loss_price}")
            
            if current_price >= take_profit_price or current_price <= stop_loss_price:
                self.place_market_order(code, qty, TrdSide.SELL)
                break
            
            time.sleep(1)  # 每秒检查一次价格

    def place_market_order(self, code, qty, trd_side):
        ret, data = self.trade_ctx.place_order(price=0, qty=qty, code=code, 
                                               trd_side=trd_side, 
                                               order_type=OrderType.MARKET, 
                                               trd_env=TrdEnv.SIMULATE)
        if ret == RET_OK:
            print(f"{'买入' if trd_side == TrdSide.BUY else '卖出'}成功: {data}")
        else:
            print(f"{'买入' if trd_side == TrdSide.BUY else '卖出'}失败: {data}")
    
    def close(self):
        self.quote_ctx.close()
        self.trade_ctx.close()

# 使用示例
if __name__ == "__main__":
    trader = PriceTriggeredTrader(unlock_password="your_trade_password")
    
    # 当价格低于等于500时买入100股，然后设置止盈价为520，止损价为480
    trade_thread = threading.Thread(target=trader.set_price_trigger, args=("HK.00700", 500, 100, 520, 480))
    trade_thread.start()
    trade_thread.join()

    trader.close()