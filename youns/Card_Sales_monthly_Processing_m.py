import DB_info as di
import timeit
import datetime
import sys
import Date as dt
import csv


cursor1 = di.dbc1.cursor()

#시간기록
sys.stdout = open(r'C:\Users\youn\card_type\card_type_' + dt.last_mm + '.csv','w')

now = datetime.datetime.now()

start_time = timeit.default_timer() # 시작 시간 체크

#카드사 결제 데이터 가공
sql1 = "INSERT INTO m_cardtype_month_sales (mai_Company_num, Card_code, Card_type, mms_year) SELECT MOI_IDX AS mai_Company_num, CD_JCODE AS Card_code, TRIM(CD_JNAME) AS Card_type, MID(sales_date,1,4) AS mms_year FROM m_cardmst WHERE CONCAT_WS('','',moi_idx, CD_JCODE, TRIM(CD_JNAME), mid(sales_date,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, Card_code, Card_type, mms_year) FROM m_cardtype_month_sales) AND mid(sales_date,1,7) = '" + dt.last_m + "' AND TRIM(CD_JNAME) NOT IN ('국세청') AND CD_JCODE <> 0 GROUP BY MOI_IDX, TRIM(CD_JNAME), MID(sales_date,1,4) ORDER BY MOI_IDX ASC, MID(sales_date,1,4) ASC;"
cursor1.execute(sql1)

#카드사 월 별 매출 데이터 가공
sql = "UPDATE m_cardtype_month_sales as A SET mms_" + dt.last_mm + "m = (SELECT NVL(SUM(B.amt_sale),0) FROM m_cardmst as B WHERE MID(B.sales_date,1,7) = '" + dt.last_m + "' AND A.mai_Company_num = B.MOI_IDX AND MID(B.sales_date,1,4) = A.mms_year AND A.Card_type = TRIM(B.CD_JNAME));"
cursor1.execute(sql)

terminate_time = timeit.default_timer()  # 종료 시간 체크
end = datetime.datetime.now()

print("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S") + " / End time : " + end.strftime("%Y-%m-%d %H:%M:%S") + " / " + dt.last_m + " 작업 완료 : " "%f초 걸렸습니다." % (terminate_time - start_time))

sys.stdout.close()

#Log 기록
lg = open(r'C:\Users\youn\card_type\card_type_' + dt.last_mm + '.csv', encoding='cp949')
csvReader_l = csv.reader(lg)

for row in csvReader_l:
    Log = (row[0])
    sql_l = "insert into m_card_type_log(Log) values ('" + str(Log) + "');"

cursor1 = di.dbc1.cursor()
cursor1.execute(sql_l)

di.dbc1.close()
