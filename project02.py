import pandas as pd
import mysql.connector
import timestamp
from mysql.connector import Error
import re
import openpyxl
import csv
import numpy as np

from pandas.core.nanops import nanall


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
def save_to_mysql(df, host, user, password, database):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="test01",
            port=3306
        )
        cursor = conn.cursor()

        # 自动创建表
        cols = df.columns
        create_table = f"""
        //to do:表不存在时自动创建
        CREATE TABLE IF NOT EXISTS user_data (
            {', '.join([f'`{col}` TEXT' for col in cols])}
        )
        """
        cursor.execute(create_table)

        # 插入数据
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO user_data ({', '.join([f'`{col}`' for col in cols])})
            VALUES ({', '.join(['%s'] * len(cols))})
            """
            cursor.execute(insert_query, tuple(row.astype(str)))

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
        # 'database': 'user_analysis'
        'port': 3306
    }

    # 执行处理流程
    # 1. 转换Excel文件为CSV（分号分隔）
    df = convert_xlsx_to_csv(input_xlsx, output_csv)
    # 2. 数据校验函数
    # 3. 正确数据传入validated_df
    validated_df = validate_data(df)
    # save_to_mysql(validated_df, **db_config)
    # analysis_result = ground_promotion_analysis(validated_df)
    # analysis_result.to_csv(result_csv, index=False)