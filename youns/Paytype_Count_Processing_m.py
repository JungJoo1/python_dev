import DB_info as di
import timeit
import datetime
import sys
import Date as dt
import csv


cursor1 = di.dbc1.cursor()

#시간기록
sys.stdout = open(r'C:\Users\youn\Pay\pay_count' + dt.last_mm + '.csv','w')

now = datetime.datetime.now()

start_time = timeit.default_timer() # 시작 시간 체크

#결제 타입 별 건수 데이터 가공

sql1 = "INSERT INTO m_cashpay_month_count (mai_Company_num, mms_year) SELECT MOI_IDX AS mai_Company_num, MID(billcode,1,4) AS mms_year FROM m_rcp_sales WHERE CONCAT_WS('','',moi_idx,mid(billcode,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year) FROM m_cashpay_month_count) AND mid(sale_date,1,7) = '" + dt.last_m + "' GROUP BY MOI_IDX, MID(billcode,1,4) ORDER BY MOI_IDX ASC, MID(billcode,1,4) ASC;"
cursor1.execute(sql1)

sql2 = "INSERT INTO m_cardpay_month_count (mai_Company_num, mms_year) SELECT MOI_IDX AS mai_Company_num, MID(billcode,1,4) AS mms_year FROM m_rcp_sales WHERE CONCAT_WS('','',moi_idx,mid(billcode,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year) FROM m_cardpay_month_count) AND mid(sale_date,1,7) = '" + dt.last_m + "' GROUP BY MOI_IDX, MID(billcode,1,4) ORDER BY MOI_IDX ASC, MID(billcode,1,4) ASC;"
cursor1.execute(sql2)

sql3 = "INSERT INTO m_misspay_month_count (mai_Company_num, mms_year) SELECT MOI_IDX AS mai_Company_num, MID(billcode,1,4) AS mms_year FROM m_rcp_sales WHERE CONCAT_WS('','',moi_idx,mid(billcode,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year) FROM m_misspay_month_count) AND mid(sale_date,1,7) = '" + dt.last_m + "' GROUP BY MOI_IDX, MID(billcode,1,4) ORDER BY MOI_IDX ASC, MID(billcode,1,4) ASC;"
cursor1.execute(sql3)

sql4 = "INSERT INTO m_pointpay_month_count (mai_Company_num, mms_year) SELECT MOI_IDX AS mai_Company_num, MID(billcode,1,4) AS mms_year FROM m_rcp_sales WHERE CONCAT_WS('','',moi_idx,mid(billcode,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year) FROM m_pointpay_month_count) AND mid(sale_date,1,7) = '" + dt.last_m + "' GROUP BY MOI_IDX, MID(billcode,1,4) ORDER BY MOI_IDX ASC, MID(billcode,1,4) ASC;"
cursor1.execute(sql4)

sql5 = "INSERT INTO m_etcpay_month_count (mai_Company_num, mms_year) SELECT MOI_IDX AS mai_Company_num, MID(billcode,1,4) AS mms_year FROM m_rcp_sales WHERE CONCAT_WS('','',moi_idx,mid(billcode,1,4)) NOT IN (SELECT DISTINCT CONCAT_WS('',mai_Company_num, mms_year) FROM m_etcpay_month_count) AND mid(sale_date,1,7) = '" + dt.last_m + "' GROUP BY MOI_IDX, MID(billcode,1,4) ORDER BY MOI_IDX ASC, MID(billcode,1,4) ASC;"
cursor1.execute(sql5)

#마트 별, 기간 별 데이터 가공
sql6 = "UPDATE m_cashpay_month_count as A SET mms_" + dt.last_mm + "m = (SELECT COUNT(B.cash) FROM m_rcp_sales as B WHERE MID(B.sale_date,1,7) = '" + dt.last_m + "' AND B.CASH > 0 AND A.mai_Company_num = B.MOI_IDX AND MID(B.billcode,1,4) = A.mms_year);"
cursor1.execute(sql6)

cs = datetime.datetime.now()

sql7 = "UPDATE m_cardpay_month_count as A SET mms_" + dt.last_mm + "m = (SELECT COUNT(B.credit_card+B.check_card) FROM m_rcp_sales as B WHERE MID(B.sale_date,1,7) = '" + dt.last_m + "' AND (B.credit_card+B.check_card) > 0 AND A.mai_Company_num = B.MOI_IDX AND MID(B.billcode,1,4) = A.mms_year);"
cursor1.execute(sql7)

cd = datetime.datetime.now()

sql8 = "UPDATE m_misspay_month_count as A SET mms_" + dt.last_mm + "m = (SELECT COUNT(B.miss) FROM m_rcp_sales as B WHERE MID(B.sale_date,1,7) = '" + dt.last_m + "' AND B.miss > 0 AND A.mai_Company_num = B.MOI_IDX AND MID(B.billcode,1,4) = A.mms_year);"
cursor1.execute(sql8)

ms = datetime.datetime.now()

sql9 = "UPDATE m_pointpay_month_count as A SET mms_" + dt.last_mm + "m = (SELECT COUNT(B.use_point) FROM m_rcp_sales as B WHERE MID(B.sale_date,1,7) = '" + dt.last_m + "' AND B.use_point >0 AND A.mai_Company_num = B.MOI_IDX AND MID(B.billcode,1,4) = A.mms_year);"
cursor1.execute(sql9)

pt = datetime.datetime.now()

sql10 = "UPDATE m_etcpay_month_count as A SET mms_" + dt.last_mm + "m = (SELECT COUNT(B.pbill_amt+B.etc_amt+B.etc_use) FROM m_rcp_sales as B WHERE MID(B.sale_date,1,7) = '" + dt.last_m + "' AND (B.pbill_amt+B.etc_amt+B.etc_use) > 0 AND A.mai_Company_num = B.MOI_IDX AND MID(B.billcode,1,4) = A.mms_year);"
cursor1.execute(sql10)

etc = datetime.datetime.now()


terminate_time = timeit.default_timer()  # 종료 시간 체크
end = datetime.datetime.now()

print("Start date and time : " + now.strftime("%Y-%m-%d %H:%M:%S") + " / Cash_count insert 완료 : " + cs.strftime("%Y-%m-%d %H:%M:%S") + " / Card_count insert 완료 : " + cd.strftime("%Y-%m-%d %H:%M:%S") + " / Miss_count insert 완료 : " + ms.strftime("%Y-%m-%d %H:%M:%S") + " / Point_count insert 완료 : " + pt.strftime("%Y-%m-%d %H:%M:%S") + " / ETC insert_count 완료 : " + etc.strftime("%Y-%m-%d %H:%M:%S") + " / End time : " + end.strftime("%Y-%m-%d %H:%M:%S") + " / " + dt.last_m + " 작업 완료 : " "%f초 걸렸습니다." % (terminate_time - start_time))

sys.stdout.close()

#Log 기록
lg = open(r'C:\Users\youn\Pay\pay_count' + dt.last_mm + '.csv', encoding='cp949')
csvReader_l = csv.reader(lg)

for row in csvReader_l:
    Log = (row[0])
    sql_l = "insert into m_pay_log(Log) values ('" + str(Log) + "');"

cursor1 = di.dbc1.cursor()
cursor1.execute(sql_l)

di.dbc1.close()
