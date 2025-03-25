import pandas as pd
import mysql.connector
import timestamp
from mysql.connector import Error
import re
import openpyxl
import csv
import numpy as np
from pandas import DataFrame

from pandas.core.nanops import nanall
from sqlalchemy.sql.sqltypes import NULLTYPE


# 转换Excel文件为CSV（分号分隔）
def convert_xlsx_to_csv(input_file, output_file):
    df = pd.read_excel(input_file,sheet_name="Sheet1", header=4)
    df.to_csv(output_file, sep=';', index=False)
    return df

# 校验电话号码是否是中国大陆手机号格式
def is_valid_phone(phone):
    """校验中国大陆手机号格式：11位，以1开头，第二位为3-9"""
    return re.fullmatch(r'^1[3-9]\d{9}$', phone) is not None

# 2. 数据校验函数
def validate_data(df, valid_data_csv=None):
    error_log = []
    valid_rows = []
    error_rows = []
    invalid_data_csv = 'D:/A_DATA/invalid_data.csv'
    valid_data_csv = 'D:/A_DATA/valid_data.csv'

    # 校验非空（external_userid）
    for index, row in df.iterrows():
        # 校验非空（external_userid）
        if pd.isna(row.get('external_userid', None)):
            error_log.append(f"行 {index + 2}: external_userid无效")
            error_rows.append(row)
        # 校验非空（客户名称）
        elif pd.isna(row.get('客户名称', None)):
            error_log.append(f"行 {index + 2}: 客户名称无效")
            error_rows.append(row)
        # 校验非空（客户名称）
        elif pd.isna(row.get('客户名称', None)):
            error_log.append(f"行 {index + 2}: 客户名称无效")
            error_rows.append(row)
        # 校验非空（所属客服）
        elif pd.isna(row.get('所属客服', None)):
            error_log.append(f"行 {index + 2}: 所属客服无效")
            error_rows.append(row)
        # 校验非空（添加时间）
        elif pd.isna(row.get('添加时间', None)):
            error_log.append(f"行 {index + 2}: 添加时间无效")
            error_rows.append(row)
        # 校验非空（添加渠道）
        elif pd.isna(row.get('添加渠道', None)):
            error_log.append(f"行 {index + 2}: 添加渠道无效")
            error_rows.append(row)
        # 校验电话号码格式有效（电话）
        elif not pd.isna(row.get('电话', None)) and row.get('电话', None) != '-' and not is_valid_phone(row.get('电话', None)):
            error_log.append(f"行 {index + 2}: 电话无效: {row.get('电话', None)}")
            error_rows.append(row)
        # 校验非空（标签组(地推专员标签)）
        elif pd.isna(row.get('标签组(地推专员标签)', None)):
            error_log.append(f"行 {index + 2}: 标签组(地推专员标签)无效")
            error_rows.append(row)
        else:
            valid_rows.append(row)

    # 写入正确数据到validated.csv
    with open(valid_data_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(valid_rows)

    # 写入错误数据到error.txt
    with open(invalid_data_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(error_rows)

    # 写入错误日志
    with open('errlog.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(error_log))

    return valid_data_csv


# 3. 存储到MySQL数据库
def save_to_mysql(df, host, user, password, database, port):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        cursor = conn.cursor()
        print(f"数据库连接成功")

        # 自动创建表
        # cols = df.index() + 1
        df: DataFrame = pd.read_csv(df)
        df = df.fillna('空')
        # df = df.where(pd.isnull(df), None)
        for index, row in df.iterrows():
            # sql = ("INSERT INTO customer_list (external_userid, customer_name, remark_name, "
            #        "churn_status, service_staff, add_time, add_channel, phone, grade, tags_grade, "
            #        "tags_course, tags_Specialist) "
            #        "VALUES (row[17], row[0], row[1], "
            #        "row[3], row[5], row[12], row[14], row[28], row[48], row[48], "
            #        "row[47], row[45])")
            sql = ("INSERT INTO customer_list (external_userid, customer_name, remark_name, "
                   "churn_status, service_staff, add_time, add_channel, phone, grade, tags_grade, "
                   "tags_course, tags_Specialist) "
                   "VALUES (%s, %s, %s, "
                   "%s, %s, %s, %s, %s, %s, %s, "
                   "%s, %s)")
            cursor.execute(sql, (row[17], row[0], row[1],
                           row[3], row[5], row[12], row[14], row[28], row[48], row[48],
                           row[47], row[49]))
        # create_table = f"""
        # //to do:表不存在时自动创建
        # CREATE TABLE IF NOT EXISTS user_data (
        #     {', '.join([f'`{col}` TEXT' for col in cols])}
        # )
        # """
        # cursor.execute(create_table)

        # 插入数据
        # for _, row in df.iterrows():
        #     insert_query = f"""
        #     INSERT INTO customer_list ({', '.join([f'`{col}`' for col in cols])})
        #     VALUES ({', '.join(['%s'] * len(cols))})
        #     """
        #     cursor.execute(insert_query, tuple(row.astype(str)))

        conn.commit()
        print(f"成功插入 {len(df)} 条数据到数据库")

    except Error as e:
        print(f"数据库错误: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


# 4. 地推专员分析
def ground_promotion_analysis(df, keywords=['地推', '专员']):
    pattern = '|'.join(keywords)

    # 过滤包含关键词的记录
    filtered = df[df['标签'].str.contains(pattern, na=False, flags=re.IGNORECASE)]

    # 计算指标
    total_users = filtered['用户数量'].sum()
    churned_users = filtered['流失数量'].sum()

    if total_users == 0:
        return pd.DataFrame()

    retention_rate = ((total_users - churned_users) / total_users) * 100
    churn_rate = (churned_users / total_users) * 100

    result = pd.DataFrame({
        '相关标签': [', '.join(keywords)],
        '用户数量': [total_users],
        '流失数量': [churned_users],
        '留存率%': [round(retention_rate, 2)],
        '流失率%': [round(churn_rate, 2)]
    })

    return result

#
# def deduplicate_csv(input_file, output_file, columns, keep):
#     """
#     根据指定列对CSV文件去重
#
#     参数：
#     - input_file:  输入CSV文件路径
#     - output_file: 输出CSV文件路径
#     - columns:     去重依据的列名（列表或单个字符串）
#     - keep:       保留策略：'first'（保留第一个）/'last'（保留最后）/'False'（删除所有重复）
#     """
#     try:
#         # 读取CSV文件
#         df = pd.read_csv(input_file)
#
#         # 执行去重操作
#         df_deduplicated = df.drop_duplicates(subset=df.columns, keep=keep)
#
#         # 保存结果
#         print(f"去重完成！原始数据 {len(df)} 行 → 去重后 {len(df_deduplicated)} 行")
#
#     except Exception as e:
#         print(f"去重处理失败：{str(e)}")


if __name__ == "__main__":
    # 参数配置
    input_xlsx = 'D:/A_DATA/input.xlsx'
    output_csv = 'D:/A_DATA/output.csv'
    result_csv = 'D:/A_DATA/analysis_result.csv'

    # 数据库配置（需要根据实际情况修改）
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'root',
        'database': 'test01',
        'port': 3306
    }

    # 执行处理流程
    # 1. 转换Excel文件为CSV（分号分隔）
    df = convert_xlsx_to_csv(input_xlsx, output_csv)
    # deduplicate_csv(
    #     input_file=df,
    #     output_file=df,
    #     columns='external_userid',  # 根据多列组合去重
    #     keep='first'  # 保留首次出现的重复项
    # )
    df = df.drop_duplicates(subset='external_userid', keep='first')
    # 2. 数据校验函数
    # 3. 正确数据传入validated_df
    validated_df = validate_data(df)
    save_to_mysql(validated_df, **db_config)
    # analysis_result = ground_promotion_analysis(validated_df)
    # analysis_result.to_csv(result_csv, index=False)