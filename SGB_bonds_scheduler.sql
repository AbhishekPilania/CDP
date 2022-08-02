create temp table SGB_Details_temp (like dbo_wms.SGB_Details);

insert into SGB_Details_temp
select distinct(client_code) as client_id,count(1) as Total_Bond_Orders,max(order_date)Last_Bond_Order,
min(order_date)First_Bond_Order from dbo_wms_s3.vw_bond_data 
where order_date=CURRENT_DATE-1
group by 1;

 begin TRANSACTION;
update
  "dbo-angels"."dbo_wms"."SGB_Details" a
set Last_Bond_Order=b.Last_Bond_Order,
Total_Bond_Orders=a.Total_Bond_Orders+b.Total_Bond_Orders
from
  SGB_Details_temp b
where
  a.client_id = b.client_id   );
  
insert into "dbo-angels"."dbo_wms"."SGB_Details"
select * from SGB_Details_temp where client_id in  (
    select
      client_id
    from
      SGB_Details_temp minus
    select
      client_id
    from
      "dbo-angels"."product_analytics"."firstlogin"
  );
end TRANSACTION;
drop table SGB_Details_temp;
