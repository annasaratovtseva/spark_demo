import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import argparse

# Получение путей до папки с исходными данными (input_folder) и папки с результатом (output_folder), введённых при запуске Spark-приложения через spark-submit
parser = argparse.ArgumentParser()
parser.add_argument("input_folder", help = "Path to the folder with input files offense_codes.csv, crime.csv")
parser.add_argument("output_folder", help = "Path to the folder with output file crimes_in_boston_analysis.parquet")
args = parser.parse_args()

# Создание SparkSession
spark = (SparkSession
    .builder
    .appName("Crimes in Boston - Analysis")
    .master("local[*]")
    .getOrCreate()
)

# Чтение данных из справочника кодов преступлений
offense_codes_df = spark.read.csv(args.input_folder + "/offense_codes.csv", header=True, inferSchema=True)
#offense_codes_df.show()

# Информация о схеме
#offense_codes_df.printSchema()

# Чтение данных о преступлениях
crime_df = spark.read.csv(args.input_folder + "/crime.csv", header=True, inferSchema=True)
#crime_df.show()

# Информация о схеме
#crime_df.printSchema()

# Регистрация DataFrame как временных представлений
offense_codes_df.createOrReplaceTempView("offense_codes")
crime_df.createOrReplaceTempView("crime")

# Проверка справочника кодов преступлений на наличие дубликатов
#result = spark.sql("""
#SELECT
#    COUNT(DISTINCT code)    AS unique_codes_number,
#    COUNT(code)             AS codes_number
#FROM offense_codes
#""")

#result.show()

# Очистка справочника кодов преступлений от дубликатов + расчёт поля crime_type
unique_offense_codes_df = spark.sql("""
SELECT
    code            AS code,
    FIRST(name)     AS name,
    CASE
        WHEN INSTR(FIRST(name), ' -') = 0
            THEN FIRST(name)
        ELSE
            SUBSTR(FIRST(name), 1, INSTR(FIRST(name), ' -') - 1)
    END             AS crime_type
FROM offense_codes
GROUP BY
    code
""")

#unique_offense_codes_df.show(truncate=False)

# Регистрация DataFrame как временного представления
unique_offense_codes_df.createOrReplaceTempView("unique_offense_codes")

# Проверка, что у всех преступлений указан код из справочника кодов преступлений
#result = spark.sql("""
#SELECT
#    COUNT(*)
#FROM crime                      C
#LEFT JOIN unique_offense_codes  UOC     ON UOC.code = C.offense_code
#WHERE UOC.code IS NULL
#""")

#result.show()

# Расчёт метрик crimes_total, lat, lng
crimes_total_lat_lng_df = spark.sql("""
SELECT
    district                            AS district,
    COUNT(DISTINCT incident_number)     AS crimes_total,
    AVG(lat)                            AS lat,
    AVG(long)                           AS lng
FROM crime
GROUP BY
    district
""")

#crimes_total_lat_lng_df.show()

# Расчёт метрики crimes_monthly
crimes_monthly_df = spark.sql("""
WITH cte AS
(
    SELECT
        district                            AS district,
        year                                AS year,
        month                               AS month,
        COUNT(DISTINCT incident_number)     AS incidents_monthly_count_by_district
    FROM crime
    GROUP BY
        district,
        year,
        month
)
SELECT
    district                                                        AS district,
    PERCENTILE_APPROX(incidents_monthly_count_by_district, 0.5)     AS crimes_monthly
FROM cte
GROUP BY
    district
""")

#crimes_monthly_df.show()

# Расчёт метрики frequent_crime_types
frequent_crime_types_df = spark.sql("""
WITH cte AS
(
    SELECT
        district                                                                                    AS district,
        offense_code                                                                                AS offense_code,
        COUNT(DISTINCT incident_number)                                                             AS incidents_count_by_district_and_offense_code,
        ROW_NUMBER() OVER (PARTITION BY district ORDER BY COUNT(DISTINCT incident_number) DESC)     AS row_number
    FROM crime
    GROUP BY
        district,
        offense_code
    ORDER BY
        district,
        row_number
)
SELECT
    T.district                                      AS district,
    CONCAT_WS(', ', COLLECT_LIST(UOC.crime_type))   AS frequent_crime_types
FROM cte                            T
INNER JOIN unique_offense_codes     UOC     ON UOC.code = T.offense_code
WHERE T.row_number <= 3
GROUP BY
    T.district
""")

#frequent_crime_types_df.show(truncate=False)

# Регистрация DataFrame как временных представлений
crimes_total_lat_lng_df.createOrReplaceTempView("crimes_total_lat_lng")
crimes_monthly_df.createOrReplaceTempView("crimes_monthly")
frequent_crime_types_df.createOrReplaceTempView("frequent_crime_types")

# Сборка витрины
data_mart_df = spark.sql("""
SELECT
    T1.district,
    T1.crimes_total,
    T2.crimes_monthly,
    T3.frequent_crime_types,
    T1.lat,
    T1.lng
FROM crimes_total_lat_lng           T1
INNER JOIN crimes_monthly           T2  ON IFNULL(T2.district, '') = IFNULL(T1.district, '')
INNER JOIN frequent_crime_types     T3  ON IFNULL(T3.district, '') = IFNULL(T1.district, '')
ORDER BY
    T1.district
""")

#data_mart_df.show(truncate=False)

# Сохранение витрины в файл crimes_in_boston_analysis.parquet
data_mart_df.write.mode("overwrite").parquet(args.output_folder + "/crimes_in_boston_analysis.parquet")

# Остановка SparkSession
spark.stop()