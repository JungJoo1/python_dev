import DB_info as di
import timeit
import datetime
import sys
import Date as dt
import csv


cursor1 = di.dbc1.cursor()

#시간기록
sys.stdout = open(r'C:\Users\youn\CJS\CJS_output_2nd_' + dt.last_mm + '.csv','w')

now = datetime.datetime.now()

start_time = timeit.default_timer() # 시작 시간 체크

#영수증 기준 데이터 가공
sql = "INSERT INTO m_rcp_sales (moi_idx,billcode ,member_no, sales_amt, cash, miss, credit_card, check_card, pbill_amt, etc_amt, use_point, etc_use, sale_date) SELECT moi_idx, CONCAT_WS('',LEFT(sales_date,4),mid(sales_date,6,2),RIGHT(sales_date,2),pos_no,rcp_no) AS billcode, member_no, sales_amt, cash, miss, credit_card, check_card, pbill_amt, etc_amt, use_point, etc_use, sale_date FROM m_cjsmst WHERE mid(sale_date,1,7) = '" + dt.last_m + "' AND cjs_idx IS NOT null;"

cursor1.execute(sql)

terminate_time = timeit.default_timer()  # 종료 시간 체크
end = datetime.datetime.now()

print("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S") + " / End time : " + end.strftime("%Y-%m-%d %H:%M:%S") + " / " + dt.last_m + " 작업 완료 : " "%f초 걸렸습니다." % (terminate_time - start_time))

sys.stdout.close()

#Log 기록
lg = open(r'C:\Users\youn\CJS\CJS_output_2nd_' + dt.last_mm + '.csv', encoding='cp949')
csvReader_l = csv.reader(lg)

for row in csvReader_l:
    Log = (row[0])
    sql_l = "insert into m_rcp_log(Log) values ('" + str(Log) + "');"

cursor1 = di.dbc1.cursor()
cursor1.execute(sql_l)

di.dbc1.close()
