# leve2转换为日频数据
### 项目介绍
将股票市场中的level2高频数据通过用python连接dolphinDB数据库，用sqol编写脚本并执行，将其转换为日频数据
### 项目步骤
- 连接dolphinDB数据库
- 用sql编写脚本并执行
- 导出处理后的日频数据
### 文件说明
- main.py 主要执行程序
- contracts/ 对委托订单的处理文件的目录
- transaction/ 对成交订单的处理文件的目录
- vwap_whole_day.py 计算全天的vwap：将交易股票的数量作为权重，计算价格的均值
- vwap_collect_time.py 计算集合竞价时间的vwap
- cwap_day.py 计算全天的cwap : 将订单的条目数作为权重，计算价格的均值
- twap_day.py 计算全天的twap : 将订单的所属时间段（这里用的是分钟级）的数量作为权重，计算价格的均值
- vmed_day.py 计算全天的vmed : 将交易股票的数量作为权重，计算价格的中位数
- cmed_day.py 计算全天的cmed : 将订单的条目数作为权重，计算价格的中位数
- tmed_day.py 计算全天的tmed : 将订单的所属时间段（这里用的是分钟级）的数量作为权重，计算价格的中位数
### 项目说明
- 该项目虽然都是py文件，但主要用的是sql对高频数据进行处理，sql脚本直接在py文件中编写
