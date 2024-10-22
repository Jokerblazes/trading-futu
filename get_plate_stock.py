from futu import *
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

ret, data = quote_ctx.get_plate_stock('HK.800000')
if ret == RET_OK:
    print(data)
    print(data['stock_name'][0])    # 取第一条的股票名称
    print(data['stock_name'].values.tolist())   # 转为 list
else:
    print('error:', data)
quote_ctx.close() # 结束后记得关闭当条连接，防止连接条数用尽
