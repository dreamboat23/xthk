import pandas as pd
import pymysql

# 1. 连接MySQL数据库
conn = pymysql.connect(
    host='localhost',      # 数据库地址
    user='root',      # 用户名
    password='root',   # 密码
    database='test01',    # 数据库名
    charset='utf8'
)

# 2. 从数据库读取数据
sql = "SELECT tags_Specialist, churn_status, external_userid FROM customer_list"
df = pd.read_sql(sql, conn)
conn.close()

# 3. 清洗数据：删除tags_Specialist为空的行
df = df.dropna(subset=['tags_Specialist'])

# ------------------ 场景1：直接统计原始标签 ------------------
result_simple = df.groupby('tags_Specialist').agg(
    用户总数=('external_userid', 'count'),
    已流失数量=('churn_status', lambda x: (x == '已流失').sum())
).reset_index()
result_simple['流失率'] = round(result_simple['已流失数量'] / result_simple['用户总数'] * 100, 2)

# ------------------ 场景2：拆分多关键词统计 ------------------
# 拆分逗号分隔的标签为多行
split_df = df.copy()
split_df['tags_Specialist'] = split_df['tags_Specialist'].str.split(',')
split_df = split_df.explode('tags_Specialist').reset_index(drop=True)

# 清理空格并过滤空值
split_df['tags_Specialist'] = split_df['tags_Specialist'].str.strip()
split_df = split_df[split_df['tags_Specialist'] != '']

# 统计结果
result_split = split_df.groupby('tags_Specialist').agg(
    用户总数=('external_userid', 'nunique'),  # 使用 nunique 去重统计
    已流失数量=('churn_status', lambda x: (x == '已流失').sum())
).reset_index()
result_split['流失率'] = round(result_split['已流失数量'] / result_split['用户总数'] * 100, 2)

# 4. 显示结果
print("【原始标签统计】")
print(result_simple)
print("\n【拆分多标签统计】")
print(result_split)
result_simple.to_excel("D:/A_DATA/原始标签统计.xlsx", index=False)
result_split.to_excel("D:/A_DATA/拆分标签统计.xlsx", index=False)