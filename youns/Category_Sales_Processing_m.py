import DB_info as di
import timeit
import datetime
import sys
import Date as dt
import csv


cursor1 = di.dbc1.cursor()

#시간기록
sys.stdout = open(r'C:\Users\youn\Cate_sales\Cate_sales_' + dt.last_mm + '.csv','w')

now = datetime.datetime.now()

start_time = timeit.default_timer() # 시작 시간 체크

#상품 별 분류 데이터 가공
sql1 = "INSERT INTO m_category_month_sales (mai_Company_num, mms_year, ATTR_CODE) SELECT A.MOI_IDX AS mai_Company_num, mid(B.sales_date,1,4) AS mms_year, A.attr1 AS ATTR_CODE FROM m_itemmst A, m_rawmst B WHERE A.moi_idx = B.moi_idx AND mid(B.sales_date,1,7) = '" + dt.last_m + "' AND A.barcode = B.barcode AND CONCAT_WS('','',A.moi_idx, MID(B.sales_date,1,4), A.attr1) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year, ATTR_CODE) FROM m_category_month_sales) GROUP BY A.MOI_IDX, mid(B.sales_date,1,4), A.attr1;"
cursor1.execute(sql1)

#기간, 상품 별 매출 정보
sql2 = "INSERT INTO m_sales_category (mai_Company_num, YY, MMS, ATTR_CODE, SALES) SELECT B.moi_idx AS mai_Company_num, MID(B.sales_date,1,4) AS YY, MID(B.sales_date,6,2) AS MMS, A.ATTR1 AS ATTR_CODE, NVL(SUM(B.tsales_amt),0) AS SALES FROM m_itemmst AS A, m_rawmst AS B WHERE A.MOI_IDX = B.MOI_IDX AND mid(B.sales_date,1,7) = '" + dt.last_m + "' AND A.barcode = B.barcode AND CONCAT_WS('','',A.moi_idx, MID(B.sales_date,1,4), MID(B.sales_date,6,2), A.attr1) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, YY, MMS, ATTR_CODE) FROM m_sales_category) GROUP BY B.moi_idx, MID(B.sales_date,1,4), MID(B.sales_date,6,2), A.ATTR1;"
cursor1.execute(sql2)

ist = datetime.datetime.now()

#월, 카테고리 별 매출 데이터 가공
sql3 = "UPDATE m_category_month_sales as A SET Category = (SELECT B.attr_name FROM m_attmst as B WHERE A.mai_Company_num = B.MOI_IDX AND A.attr_code = B.attr1 AND B.attr2 = '0' AND B.attr3 = '0' AND A.mms_year = 'mid(" + dt.last_m + ",1,4)' GROUP BY A.mai_Company_num, A.attr_code);"
cursor1.execute(sql3)

sql4 = "UPDATE m_category_month_sales as A SET mms_" + dt.last_mm + "m = (SELECT NVL(SUM(B.SALES),0) FROM m_sales_category as B WHERE B.mms = '" + dt.last_mm + "' AND A.mai_Company_num = B.mai_Company_num AND B.yy = 'mid(" + dt.last_m + ",1,4)' AND B.yy = A.mms_year AND A.ATTR_CODE = B.ATTR_CODE);"
cursor1.execute(sql4)

up = datetime.datetime.now()

terminate_time = timeit.default_timer()  # 종료 시간 체크
end = datetime.datetime.now()

print("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S") + " / Category insert 완료 : " + ist.strftime("%Y-%m-%d %H:%M:%S") + " / Sales update 완료 : " + up.strftime("%Y-%m-%d %H:%M:%S") + " / End time : " + end.strftime("%Y-%m-%d %H:%M:%S") + " / " + dt.last_m + " 작업 완료 : " "%f초 걸렸습니다." % (terminate_time - start_time))

sys.stdout.close()

#Log 기록
lg = open(r'C:\Users\youn\Cate_sales\Cate_sales_' + dt.last_mm + '.csv', encoding='cp949')
csvReader_l = csv.reader(lg)

for row in csvReader_l:
    Log = (row[0])
    sql_l = "insert into m_catesales_log(Log) values ('" + str(Log) + "');"

cursor1 = di.dbc1.cursor()
cursor1.execute(sql_l)

di.dbc1.close()
