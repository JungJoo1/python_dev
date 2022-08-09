import DB_info as di
import timeit
import datetime
import sys
import Date as dt
import csv


cursor1 = di.dbc1.cursor()

#시간기록
sys.stdout = open(r'C:\Users\youn\Puc_mon\Puc_mon_' + dt.last_mm + '.csv','w')

now = datetime.datetime.now()

start_time = timeit.default_timer() # 시작 시간 체크

#매입 별 분류 데이터 가공
# sql1 = "INSERT INTO m_puc_item (mai_Company_num, cst_code, CST_NAME, barcode, bar_desc, ATTR_CODE, CATEGORY, mms_year, mms_mon, puc_amt) SELECT A.MOI_IDX AS mai_Company_num, B.cst_code AS cst_code, D.CST_NAME AS CST_NAME, A.barcode AS barcode, replace(A.bar_desc, '/', '') AS bar_desc, A.attr1 AS ATTR_CODE, C.CATEGORY AS CATEGORY, MID(puc_date,1,4) AS mms_year, MID(puc_date,6,2) AS mms_mon, sum(B.puc_amt) AS puc_amt FROM m_itemmst A, m_pucmst B, m_category_month_sales C, m_grcmst D WHERE A.moi_idx = B.moi_idx AND A.moi_idx = C.mai_Company_num AND A.barcode = B.barcode AND A.ATTR1 = C.ATTR_CODE AND B.MOI_IDX = D.MOI_IDX AND B.CST_CODE = D.CST_CODE AND CONCAT_WS('','', A.moi_idx, MID(B.puc_date,1,4), MID(B.puc_date,6,2), A.attr1) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year, mms_mon, Attr_code) FROM m_puc_item) GROUP BY A.MOI_IDX, MID(puc_date,1,4), MID(puc_date,6,2), A.barcode;"
# cursor1.execute(sql1)

# # 월 별 매입 정보
# sql2 = "INSERT INTO m_puc_month (mai_Company_num, cst_code, CST_NAME, ATTR_CODE, CATEGORY, KEYCODE, mms_year) SELECT A.MOI_IDX AS mai_Company_num, B.cst_code AS cst_code, D.CST_NAME AS CST_NAME, A.attr1 AS ATTR_CODE, C.CATEGORY AS CATEGORY, CONCAT_WS('',B.cst_code,A.attr1,MID(B.puc_date,1,4)) AS KEYCODE, MID(B.puc_date,1,4) AS mms_year FROM m_itemmst A, m_pucmst B, m_category_month_sales C, m_grcmst D WHERE A.moi_idx = B.moi_idx AND A.moi_idx = C.mai_Company_num AND A.barcode = B.barcode AND A.ATTR1 = C.ATTR_CODE AND B.MOI_IDX = D.MOI_IDX AND B.CST_CODE = D.CST_CODE AND CONCAT_WS('','', A.moi_idx, MID(B.puc_date,1,4), A.attr1) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year, Attr_code) FROM m_puc_month) GROUP BY A.MOI_IDX, MID(B.puc_date,1,4), MID(B.puc_date,6,2), A.ATTR1, B.CST_CODE;"
# cursor1.execute(sql2)

ist = datetime.datetime.now()

#월, 거래처 별 매입 데이터 가공
for i in range(1,13):
    sql3 = "UPDATE m_puc_month as A SET mms_" + str(i).zfill(2) + "m = (SELECT NVL(SUM(B.puc_amt),0) FROM m_puc_item as B WHERE B.mms_mon = " + str(i).zfill(2) + " AND A.mai_Company_num = B.mai_Company_num AND A.KEYCODE = CONCAT_WS('',B.cst_code,B.ATTR_CODE,B.mms_year));"
    cursor1.execute(sql3)

up = datetime.datetime.now()

terminate_time = timeit.default_timer()  # 종료 시간

end = datetime.datetime.now()

print("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S") + " / Purchase insert 완료 : " + ist.strftime("%Y-%m-%d %H:%M:%S") + " / Purchase update 완료 : " + up.strftime("%Y-%m-%d %H:%M:%S") + " / End time : " + end.strftime("%Y-%m-%d %H:%M:%S") + " / " + dt.last_m + " 작업 완료 : " "%f초 걸렸습니다." % (terminate_time - start_time))

sys.stdout.close()

#Log 기록
lg = open(r'C:\Users\youn\Puc_mon\Puc_mon_' + dt.last_mm + '.csv', encoding='cp949')
csvReader_l = csv.reader(lg)

for row in csvReader_l:
    Log = (row[0])
    sql_l = "insert into m_puc_mon_log(Log) values ('" + str(Log) + "');"

cursor1 = di.dbc1.cursor()
cursor1.execute(sql_l)

di.dbc1.close()
