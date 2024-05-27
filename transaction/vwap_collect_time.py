import dolphindb as ddb
import pandas as pd
import numpy as np


def cal(date, path, old_exist=True):
    # 连接dolphinDB
    s = ddb.session()
    conn = s.connect("ip", port, "user_name", "password")

    # 运行脚本
    script = """
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
open_vwap = table(`600000 as SecurityID, 0 as vwap);

for(date in sub_list){
	T = select * from t where TickTime between timestamp(string(date) + "T09:15:00.000") and timestamp(string(date) + "T09:25:00.000") and Type == 'T' and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 
	//计算该日各股票的vwap
	data = select SecurityID, Price, Qty from T;
	Q = select sum(Qty) from data group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	open_vwap = out_join(open_vwap, res, date, `res_vwap);
	}
	
//删除列
drop!(open_vwap, `vwap);


//收盘集合竞价
close_vwap = table(`600000 as SecurityID, 0 as vwap);

for(date in sub_list){
	T = select * from t where TickTime between timestamp(string(date) + "T15:00:00.000") and timestamp(string(date) + "T15:05:00.000") and Type == 'T' and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 
	//计算该日各股票的vwap
	data = select SecurityID, Price, Qty from T;
	Q = select sum(Qty) from data group by SecurityID, Price;
	res = select SecurityID, wavg(Price, sum_Qty) as vwap from Q group by SecurityID order by SecurityID;
	//外连接
	close_vwap = out_join(close_vwap, res, date, `res_vwap);
	}
	
//删除列
drop!(close_vwap, `vwap);
    """
    s.run(script)

    # 脚本输出
    open_vwap = s.run("open_vwap")
    res = open_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "transaction/vwap_open_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)

    close_vwap = s.run("close_vwap")
    res = close_vwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "transaction/vwap_close_time.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)
