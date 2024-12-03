CREATE TABLE IF NOT EXISTS stock_ma_status (
    index_code TEXT,
    stock_code TEXT,
    date DATE,
    above_ma_50 BOOLEAN,
    PRIMARY KEY (index_code, stock_code, date)
);