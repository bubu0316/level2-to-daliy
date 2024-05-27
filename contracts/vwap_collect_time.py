import dolphindb as ddb
import pandas as pd
import numpy as np


def cal(date, path, old_exist=True):
    # 连接dolphinDB
    s = ddb.session()
    conn = s.connect("ip", port, "user_name", "password")

    # 运行脚本
    script = """
//卖方追溯委托
def sell_order(data)
{
	s_data = select * from data where SellOrderNo > 0 order by SecurityID, SellOrderNo asc;
	//新增 - 撤单
	s_ad = select * from s_data where Type == "A";
	s_de = select * from s_data where Type == "D";
	s_temp = lj(s_ad, s_de, `SecurityID `SellOrderNo);
	update s_temp set s_de_Qty = s_de_Qty.nullFill(0);
	update s_temp set Q = Qty - s_de_Qty;
	s_new_ad = select SecurityID, Price, Q, BuyOrderNo, SellOrderNo, TickTime from s_temp where Q > 0;
	//从成交订单追溯委托订单
	s_deal = select SecurityID, Price, Qty, BuyOrderNo, SellOrderNo, TickTime from s_data where Type == "T";
	s_d_sum = select min(TickTime), max(Price), sum(Qty) as Q from s_deal group by SecurityID, SellOrderNo;
	s_out = fj(s_new_ad, s_d_sum, `SecurityID `SellOrderNo);
	update s_out set Q = Q.nullFill(s_d_sum_Q);
	update s_out set Price = Price.nullFill(max_Price);
	update s_out set SecurityID = SecurityID.nullFill(s_d_sum_SecurityID);
	update s_out set TickTime = TickTime.nullFill(min_TickTime);
	update s_out set SellOrderNo = SellOrderNo.nullFill(s_d_sum_SellOrderNo);
	update s_out set BuyOrderNo = BuyOrderNo.nullFill(0);
	s_out = select SecurityID, Price, Q as Qty, BuyOrderNo, SellOrderNo, TickTime from s_out order by SecurityID, TickTime;
	return s_out;
}

//买方委托追溯
def buy_order(data)
{
	b_data = select * from data where SellOrderNo > 0 order by SecurityID, SellOrderNo asc;
	//新增 - 撤单
	b_ad = select * from b_data where Type == "A";
	b_de = select * from b_data where Type == "D";
	b_temp = lj(b_ad, b_de, `SecurityID `SellOrderNo);
	update b_temp set b_de_Qty = b_de_Qty.nullFill(0);
	update b_temp set Q = Qty - b_de_Qty;
	b_new_ad = select SecurityID, Price, Q, BuyOrderNo, SellOrderNo, TickTime from b_temp where Q > 0;
	//从成交订单追溯委托订单
	b_deal = select SecurityID, Price, Qty, BuyOrderNo, SellOrderNo, TickTime from b_data where Type == "T";
	b_d_sum = select min(TickTime), min(Price), sum(Qty) as Q from b_deal group by SecurityID, SellOrderNo;
	b_out = fj(b_new_ad, b_d_sum, `SecurityID `SellOrderNo);
	update b_out set Q = Q.nullFill(b_d_sum_Q);
	update b_out set Price = Price.nullFill(min_Price);
	update b_out set SecurityID = SecurityID.nullFill(b_d_sum_SecurityID);
	update b_out set TickTime = TickTime.nullFill(min_TickTime);
	update b_out set SellOrderNo = SellOrderNo.nullFill(b_d_sum_SellOrderNo);
	update b_out set BuyOrderNo = BuyOrderNo.nullFill(0);
	b_out = select SecurityID, Price, Q as Qty, BuyOrderNo, SellOrderNo, TickTime from b_out order by SecurityID, TickTime;
	return b_out;
}

def out_join(pt, res, date, col_name)
{
	//外连接
	new_pt = fj(pt, res, `SecurityID);
	update new_pt set SecurityID = SecurityID.nullFill(res_SecurityID);
	new_pt = drop!(new_pt, `res_SecurityID);
	//修改列名
	new_date = concat("date_", string(date));
	new_date = strReplace(new_date, ".", "_");
	rename!(new_pt, col_name, new_date);
	return new_pt;	
}

//导入数据
t = loadTable("dfs://TL_Level2", "TL_mdl_4_24_0"); 

//日期列表
date_list = t.schema().partitionSchema[0];
sub_list = date_list[find(date_list, """ + date + """):];

//开盘集合竞价
open_s_vwap = table(`600000 as SecurityID, 0 as vwap);
open_b_vwap = table(`600000 as SecurityID, 0 as vwap);

for(date in sub_list)
{
	data = select SecurityID, Price, Qty, Type, BuyOrderNo, SellOrderNo, TickTime from t where TickTime between timestamp(string(date) + "T09:15:00.000") and timestamp(string(date) + "T09:25:00.000")  and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 

	//卖方
	s_out = sell_order(data);
	//计算vwap
	Q = select sum(Qty) from s_out group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	open_s_vwap = out_join(open_s_vwap, res, date, `res_vwap);

	//买方
	b_out = buy_order(data);
	//计算vwap
	Q = select sum(Qty) from b_out group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	open_b_vwap = out_join(open_b_vwap, res, date, `res_vwap);
}

//删除列
drop!(open_s_vwap, `vwap);
drop!(open_b_vwap, `vwap);

//收盘集合竞价
close_s_vwap = table(`600000 as SecurityID, 0 as vwap);
close_b_vwap = table(`600000 as SecurityID, 0 as vwap);

for(date in sub_list)
{
	data = select SecurityID, Price, Qty, Type, BuyOrderNo, SellOrderNo, TickTime from t where TickTime between timestamp(string(date) + "T15:00:00.000") and timestamp(string(date) + "T15:05:00.000")  and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 

	//卖方
	s_out = sell_order(data);
	//计算vwap
	Q = select sum(Qty) from s_out group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	close_s_vwap = out_join(close_s_vwap, res, date, `res_vwap);

	//买方
	b_out = buy_order(data);
	//计算vwap
	Q = select sum(Qty) from b_out group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	close_b_vwap = out_join(close_b_vwap, res, date, `res_vwap);
}

//删除列
drop!(close_s_vwap, `vwap);
drop!(close_b_vwap, `vwap);
    """
    s.run(script)

    # 脚本输出
    open_s_vwap = s.run("open_s_vwap")
    res = open_s_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "contracts/sell/vwap_open_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)

    open_b_vwap = s.run("open_b_vwap")
    res = open_b_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "contracts/buy/vwap_open_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)

    close_s_vwap = s.run("close_s_vwap")
    res = close_s_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "contracts/sell/vwap_close_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)

    close_b_vwap = s.run("close_b_vwap")
    res = close_b_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "contracts/buy/vwap_close_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)
