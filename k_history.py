from futu import *

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

ret, data, page_req_key = quote_ctx.request_history_kline('HK.00700', start='2023-01-01', end='2023-12-31', max_count=100)

if ret == RET_OK:
    print(data)
    print(f"第一条记录的股票代码: {data['code'][0]}")
    print(f"收盘价列表: {data['close'].values.tolist()}")
else:
    print('error:', data)
    
while page_req_key != None:  # 请求后续页面
    print('*' * 50)
    ret, data, page_req_key = quote_ctx.request_history_kline('HK.00700', start='2023-01-01', end='2023-12-31', max_count=100, page_req_key=page_req_key)
    if ret == RET_OK:
        print(data)
    else:
        print('error:', data)

print('所有数据获取完毕!')

quote_ctx.close()  # 结束后记得关闭连接
