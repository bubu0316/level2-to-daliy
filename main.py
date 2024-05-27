import transaction.cmed_day
import transaction.cwap_day
import transaction.tmed_day
import transaction.twap_day
import transaction.vmed_day
import transaction.vwap_whole_day
import transaction.vwap_collect_time
import contracts.cmed_day
import contracts.cwap_day
import contracts.tmed_day
import contracts.twap_day
import contracts.vmed_day
import contracts.vwap_whole_day
import contracts.vwap_collect_time
import os


def output(date, path, old_exist):
    transaction.cmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.cmed_day -> success")
    transaction.cwap_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.cwap_day -> success")
    transaction.tmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.tmed_day -> success")
    transaction.twap_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.twap_day -> success")
    transaction.vmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.vmed_day -> success")
    transaction.vwap_whole_day.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.vwap_whole_day -> success")
    transaction.vwap_collect_time.cal(date=date, path=path, old_exist=old_exist)
    print("transaction.vwap_collect_time -> success")
    contracts.cmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.cmed_day -> success")
    contracts.cwap_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.cwap_day -> success")
    contracts.tmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.tmed_day -> success")
    contracts.twap_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.twap_day -> success")
    contracts.vmed_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.vmed_day -> success")
    contracts.vwap_whole_day.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.vwap_whole_day -> success")
    contracts.vwap_collect_time.cal(date=date, path=path, old_exist=old_exist)
    print("contracts.vwap_collect_day -> success")


if __name__ == '__main__':
    # 日期
    date = "update_date"
    # 输出路径
    path = "path to output"
    # 是否已存在历史文件
    old_exist = True

    # 创建目录
    if not os.path.exists(path + "contracts"):
        os.mkdir(path + "contracts")
    if not os.path.exists(path + "contracts/buy"):
        os.mkdir(path + "contracts/buy")
    if not os.path.exists(path + "contracts/sell"):
        os.mkdir(path + "contracts/sell")
    if not os.path.exists(path + "transaction"):
        os.mkdir(path + "transaction")

    # 执行函数
    output(date, path, old_exist)
