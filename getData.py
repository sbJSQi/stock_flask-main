"""
通过爬虫获取自1991年至今天‘上证指数’，‘A股指数’，‘深证综指’,'沪深300‘的历史数据
"""
import pandas as pd
import requests
from sqlalchemy import create_engine

def getUrl(stock_name: list, stock_code: list, start_date: list, end_date: str) -> dict:
    """
    获取请求的url
    :return: 封装好的请求url的字典
    """
    # 请求数据的源地址
    base_url = 'https://q.stock.sohu.com/hisHq?'
    temp_url_list = []      # 暂存组成的单个请求地址
    url_dict = {}           # 初始化返回的字典
    for date in start_date:
        if date == '19910102':
            for i in range(2):
                temp_url = base_url + f'code=zs_{stock_code[i]}&start={date}&end={end_date}'
                temp_url_list.append(temp_url)
        elif date == '19910405':
            temp_url = base_url + f'code=zs_{stock_code[2]}&start={date}&end={end_date}'
            temp_url_list.append(temp_url)
        else:
            temp_url = base_url + f'code=zs_{stock_code[3]}&start={date}&end={end_date}'
            temp_url_list.append(temp_url)
    # 组合URL字典
    for i, name in enumerate(stock_name):
        url_dict[name] = temp_url_list[i]
    return url_dict
#数据库追加数据功能，检查数据是否存在于表中，仅添加最新数据
def getData(url_dict: dict, stock_name: list[str]):
    """
    获取股票历史数据，并追加到MySQL数据库中
    :return: 无返回值
    """
    # 数据库连接信息，根据实际情况填写
    db_info = {
        'user': 'root',
        'password': '123456',
        'host': 'localhost',
        'port': '3306',
        'database': 'stock_flask'
    }
    # 创建数据库连接引擎
    engine = create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}")
    # 定义Excel的表头
    columns = ['日期', '开盘', '收盘', '涨跌额', '涨跌幅', '最低', '最高', '成交量(手)', '成交金额(万)', '换手率']
    for name in stock_name:
        response = requests.get(url=url_dict[name])
        # 将请求到的数据转换为json格式
        response_json = response.json()
        # 将获取的数据转换为pandas所支持的dataframe数据对象
        data_frame = pd.DataFrame(data=response_json[0]['hq'], columns=columns)
        # 存入数据库
        table_name = f"{name}历史数据"
        
        # 检查表是否存在，如果不存在则创建表
        if not engine.has_table(table_name):
            data_frame.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
            continue
        
        # 获取数据库中已有数据的最新日期
        last_date_sql = f"SELECT `日期` FROM `{table_name}` ORDER BY `日期` DESC LIMIT 1"
        last_date_df = pd.read_sql(last_date_sql, engine)
        last_date = last_date_df.iloc[0]['日期'] if not last_date_df.empty else None
        
        # 如果有最新日期，则筛选出新数据
        if last_date:
            new_data = data_frame[data_frame['日期'] > last_date]
            if not new_data.empty:
                # 追加新数据到数据库
                new_data.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        else:
            # 如果数据库为空，则添加所有数据
            data_frame.to_sql(name=table_name, con=engine, index=False, if_exists='append')


def getData(url_dict: dict, stock_name: list[str]):
    """
    获取股票历史数据，并存入MySQL数据库
    :return: 无返回值
    """
    # 数据库连接信息，根据实际情况填写
    db_info = {
        'user': 'root',
        'password': '123456',
        'host': 'localhost',
        'port': '3306',
        'database': 'stock_flask'
    }
    # 创建数据库连接引擎
    engine = create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}")
    # 定义Excel的表头
    columns: list[str] = ['日期', '开盘', '收盘', '涨跌额', '涨跌幅', '最低', '最高', '成交量(手)', '成交金额(万)', '换手率']
    for name in stock_name:
        response = requests.get(url=url_dict[name])
        # 将请求到的数据转换为json格式
        response_json = response.json()
        # 将获取的数据转换为pandas所支持的dataframe数据对象
        data_frame = pd.DataFrame(data=response_json[0]['hq'], columns=columns)
        # 存入数据库
        table_name = f"{name}历史数据"
        # 如果表存在，则删除表
        if engine.has_table(table_name):
            engine.execute(f"DROP TABLE {table_name}")
        # 保存数据到数据库
        data_frame.to_sql(name=table_name, con=engine, index=False)

if __name__ == '__main__':
    pass
