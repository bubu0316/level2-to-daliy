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

//创建表来保存结果
cwap = table(`600000 as SecurityID, 0 as cwap);

for(date in sub_list){
	T = select * from t where TickTime between timestamp(string(date) + "T00:00:00.000") and timestamp(string(date) + "T23:59:59.999") and Type == 'T' and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 
	//计算该日各股票的cwap
	count = select count(Price) from T group by SecurityID, Price order by SecurityID asc;
	res = select SecurityID, wavg(Price, count_Price) as cwap from count group by SecurityID order by SecurityID;
	//外连接
	cwap = out_join(cwap, res, date, `res_cwap);
	}
	
//删除列
drop!(cwap, `cwap);
    """
    s.run(script)

    # 脚本输出
    cwap = s.run("cwap")
    res = cwap.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "transaction/cwap_day.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)
