# social_data_analysis


📊 Simulated Social Platform Communication Log Big Data Analysis System (Hive + FineBI)
This project builds a simulated communication log analysis system based on the Hive data warehouse and the FineBI visualization platform. Using Python, we generated millions of communication records. HiveSQL was then applied for data cleaning and aggregation, and FineBI was used to visualize user distribution, communication frequency, top users, and more.

📁 Project Structure
data_generator.py: A Python script that generates 1 million+ simulated communication records, including Chinese names, GPS coordinates, message content, and more.

china_province.py: To identify which province each message belongs to based on its GPS coordinates, this Python script reads from a china_province.json GeoJSON file and extracts the bounding boxes (latitude and longitude ranges) for each province.

hive_sql/: Contains SQL scripts for creating Hive tables, cleaning the data, and performing statistical analysis. These scripts are optimized for use with FineBI.

README.md: Project documentation.

screenshots/: Contains screenshots of the FineBI dashboards and visual reports.

🏗️ Hadoop Distributed Environment Support
The project runs on a Hadoop 3.3.6 distributed data processing platform:

A pseudo-distributed cluster is set up with node1 (master node), node2, and node3.

All Hive tables are stored in HDFS, enabling efficient read/write operations for large-scale data.

Hive metadata is managed by Metastore, which is deployed on node1 and accessible remotely via JDBC by DataGrip and FineBI.

MapReduce serves as the execution engine for HiveSQL, improving processing efficiency for million-level datasets.

🔍 Data Processing Workflow
Data Generation: Use Python Faker to simulate communication logs in CSV format (including null values, abnormal GPS, and various device models).

Data Ingestion: Import the CSV files into Hive internal tables using LOAD DATA, and define cleaning table schemas.

Data Processing: Perform province mapping, statistical analysis, and top user extraction using HiveSQL.

Query Testing: Write and validate SQL queries using DataGrip to ensure correctness and performance.

Visualization: Connect Hive tables to FineBI and design interactive visual reports using maps, radar charts, bar charts, etc.

📈 Visualization Highlights (FineBI)
Geographic Distribution of Communication Users: Based on GPS matching to provinces.

Monthly Top 5 Senders Radar Chart: Supports month switching for dynamic comparison.

Monthly Statistics of Senders and Receivers: Displayed via bar charts.

📦 Environment Information
Hadoop: 3.3.6

Hive: Supports remote connections via Beeline

Python: 3.10 (using Faker, csv libraries)

FineBI: Locally deployed, connected to Hive Metastore

DataGrip: Used to write and validate HiveSQL queries

System Environment: Ubuntu virtual machines (for Hive and Hadoop deployment)

模拟社交平台通信日志大数据分析系统（Hive + FineBI） 

本项目基于 Hive 数据仓库与 FineBI 可视化平台，构建了一个模拟的通信日志分析系统。通过 Python 模拟生成百万级通信数据，使用 HiveSQL 进行数据清洗和统计，最终在 FineBI 上展示用户分布、通信频率、热门用户等可视化报表。

📁 项目结构

data_generator.py：用于生成 100 万级通信数据的 Python 脚本，包含中文姓名、GPS、通信内容等字段。

china_province.py: 为了在 Hive SQL 中根据经纬度判断每条消息属于哪个省份，本项目使用以下 Python 脚本读取 china_province.json GeoJSON 文件，并提取各省的经纬度范围（bounding box）：

hive_sql/：Hive 表的创建、清洗、统计 SQL 脚本，适配 FineBI 使用。

README.md：项目说明文档。

screenshots/：FineBI 报表截图。

🏗️ Hadoop 分布式环境支持

本项目构建在 Hadoop 3.3.6 分布式数据处理平台之上：

搭建了包含 node1（主节点）、node2、node3 的伪分布式集群。

所有 Hive 表的数据存储在 HDFS 上，支持大规模数据的高效读写。

Hive 元数据由 Metastore 维护，部署在 node1，并通过 JDBC 供 DataGrip 与 FineBI 远程访问。

MapReduce 引擎支撑了 HiveSQL 的底层查询执行，提高了百万级数据的处理效率。

🔍 数据处理流程

使用 Python Faker 模拟生成 CSV 格式的通信日志数据（包含 null 值、异常经纬度、不同机型等）。

通过 Hive LOAD DATA 导入数据至内部表并创建清洗表结构。

执行 HiveSQL 对数据进行分省划分、统计分析、TOP 用户提取等。

使用 DataGrip 编写并测试 SQL，确保查询性能与正确性。

在 FineBI 中接入 Hive 表，设计地图、雷达图、柱状图等报表实现数据可视化。

📈 可视化亮点（FineBI）

通信用户地域分布图（基于经纬度匹配省份）。

月度热门发送用户 TOP5 雷达图（支持月份切换）。

每月收发用户数量统计图（柱状图）。

📦 环境说明

Hadoop: 3.3.6

Hive: 支持 Beeline 远程连接

Python: 3.10，依赖 Faker、csv

FineBI: 本地部署，连接 Hive Metastore

DataGrip: 编写并验证 HiveSQL 查询

系统环境: Ubuntu 虚拟机（Hive 和 Hadoop 部署）
