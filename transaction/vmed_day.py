import dolphindb as ddb
import pandas as pd
import numpy as np


def cal(date, path, old_exist=True):
    # 连接dolphinDB
    s = ddb.session()
    conn = s.connect("ip", port, "user_name", "password")

    # 运行脚本
    script = """
def cal_med(data, key)
{	
	temp = data
	total = select sum(weight) from temp group by SecurityID;
	weight = lj(temp, total, `SecurityID);
	update weight set iweight = cast(weight, float) / cast(sum_weight, float);
	update weight set cum = cumsum(iweight);
	sup = select 0*count(Price) from weight group by SecurityID;
	update sup set mul = mul + 1;
	update sup set i = cumsum(mul);
	update sup set i = i - 1;
	weight = lj(weight, sup, `SecurityID);
	weight = select SecurityID, Price, cum - i as cumsum from weight;
	weight = select * from weight where cumsum >= 0.5;
	min_wt = select SecurityID, min(cumsum) from weight group by SecurityID;
	update min_wt set cumsum = min_cumsum;
	res = lj(min_wt, weight, `SecurityID `cumsum);
	res = select SecurityID, Price from res;
	rename!(res, `Price, key);
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
vmed = table(`600000 as SecurityID, 0 as vmed);

for(date in sub_list){
	T = select * from t where TickTime between timestamp(string(date) + "T00:00:00.000") and timestamp(string(date) + "T23:59:59.999") and Type == 'T' and (startsWith(SecurityID, "00") or startsWith(SecurityID, "3") or startsWith(SecurityID, "6")); 
	//计算vmed
	temp = select SecurityID, Price, sum(Qty) as weight from T group by SecurityID, Price order by SecurityID, Price;
	res = cal_med(temp, `vmed);
	//外连接
	vmed = out_join(vmed, res, date, `res_vmed);
	}
	
//删除列
drop!(vmed, `vmed);
    """
    s.run(script)

    # 脚本输出
    vmed = s.run("vmed")
    res = vmed.set_index("SecurityID").rename(columns=lambda x: x.replace('date_', '')).rename(columns=lambda x: x.replace('_', '-')).T.rename_axis("date")

    file_path = path + "transaction/vmed_day.csv"
    if old_exist:
        old = pd.read_csv(file_path, index_col=0)
        new = pd.concat([old, res])
    else:
        new = res
    new = new.sort_index(axis=1)
    new.to_csv(file_path)
