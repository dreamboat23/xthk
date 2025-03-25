# xthk
data
【第一阶段】
输入：xlsx
1、转换成csv文件
2、按行存储数据（字段1；字段2.....）
3、校验数据（输出结果到errlog）
4、存入数据库
5、根据地推专员关键词检索流失率/留存率（地推专员相关标签（逗号分隔）、用户数量、流失数量、留存率、流失率）
输出：csv文件

mysql -h localhost -u root -p 

数据中心前期工作准备结果及流程
1、工具：mysql、dbeaver、fork、PyCharm
MySQL数据库下载及安装教程（最最新版）
https://blog.csdn.net/weixin_39289696/article/details/128850498
https://dbeaver.io/download/
Python+PyCharm的安装步骤及PyCharm的使用（含快捷键）
https://blog.csdn.net/junleon/article/details/120698578
FORK客户端使用教程
https://blog.csdn.net/JohnJim0/article/details/104725963

Git远程仓库：
git@github.com:dreamboat23/xthk.git

流程：
数据库创建：
CREATE TABLE `customer_list`  (
  `external_userid` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '用户id',
  `customer_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '客户名称',
  `remark_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '备注名',
  `churn_status` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '流失状态（\"已流失\"/\"未流失\"',
  `service_staff` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '所属客服',
  `add_time` datetime NOT NULL COMMENT '添加时间',
  `add_channel` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '添加渠道',
  `phone` varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '电话',
  `grade` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '就读年级（如\"小班\"、\"二年级\"等）',
  `tags_grade` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '年级相关标签（逗号分隔）',
  `tags_course` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '课程相关标签（逗号分隔）',
  `tags_Specialist` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '地推专员相关标签（逗号分隔）',
  PRIMARY KEY (`external_userid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

清除数据库数据：
mysql> truncate customer_list;

执行
project02.py
输入：xlsx
input_xlsx = 'D:/A_DATA/input.xlsx'
1、转换成csv文件
2、按行存储数据（字段1；字段2.....）
3、校验数据（输出结果到errlog）
4、存入数据库

churn_analysis02.py
5、根据地推专员关键词检索流失率/留存率（地推专员相关标签（逗号分隔）、用户数量、流失数量、留存率、流失率）
输出：csv文件
result_simple.to_excel("D:/A_DATA/原始标签统计.xlsx", index=False)
result_split.to_excel("D:/A_DATA/拆分标签统计.xlsx", index=False)


可改进点：
1、每月数据可单独创建一个表
命名格式：xthk_customer_list_202502
2、执行时可选择外部传入参数的形式将输入文件位置灵活变动
执行命令：python project02.py D:/A_DATA/input.xlsx
3、输入文件应保持关键词部分在固定的前面几列，因为程序导入MySQL数据库时会根据列的序号传入