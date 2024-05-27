import dolphindb as ddb
import pandas as pd
import numpy as np


def cal(date, path, old_exist=True):
    # 连接dolphinDB
    s = ddb.session()
    conn = s.connect("ip", port, "user_name", "password")

    # 运行脚本
    script = """
def cal_tmed(data)
{	
	temp = select SecurityID, Price, Qty, TickTime from data;
	update temp set minute = minute(TickTime);
	minu = select SecurityID, minute, wavg(Price, Qty) as Price from temp group by SecurityID, minute order by SecurityID, Price asc;
	res = select SecurityID, median(Price) as tmed from minu group by SecurityID;
	return res;
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

//创建表来保存结果
tmed = table(`600000 as SecurityID, 0 as tmed);

for(date in sub_list){
	T = select * from t where TickTime between timestamp(string(date) + "T00:00:00.000") and timestamp(string(date) + "T23:59:59.999") and Type == 'T' and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 
	//计算tmed
	res = cal_tmed(T);
	//外连接
	tmed = out_join(tmed, res, date, `res_tmed);
	}
	
//删除列
drop!(tmed, `tmed);
    """
    s.run(script)

    # 脚本输出
    tmed = s.run("tmed")
    res = tmed.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "transaction/tmed_day.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)
