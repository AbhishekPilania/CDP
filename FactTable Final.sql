create temp table client_fact_stg (like product_analytics.client_fact);
insert into
  Client_fact_stg
        with Master as(
    select
      *
    from
      (
        select
          party_code as client_id,
          applicationno as application_no,
          initcap(clientname) clientname,
        case
        when kyc_type in ('Online', 'Referral', 'Other') then 'B2C'
        when kyc_type = 'DRA' then 'DRA' else 'other'
      end as b2c_dra_tag,
              case
        when kyc_type = 'Online'
        and online_Source IN (
          'ABMA_Organic',
          'ABMA_organic',
          'Web',
          'Organic_referral'
        ) then 'Organic'
        when kyc_type = 'Online'
        and online_Source not IN (
          'ABMA_Organic',
          'ABMA_organic',
          'Web',
          'Organic_referral'
        ) then 'Paid'
        when kyc_type = 'DRA' THEN 'DRA'
        when kyc_type in ('Referral', 'Other') then 'Referral'
      end as L1_tags,
      case
        when kyc_type = 'Online'
        and online_Source IN ('ABMA_Organic', 'ABMA_organic') then 'ABMA_Organic'
        when kyc_type = 'Online'
        and online_Source IN ('Web', 'Organic_referral') then online_Source
        when kyc_type = 'Online'
        and online_Source NOT IN (
          'ABMA_Organic',
          'ABMA_organic',
          'Web',
          'Organic_referral'
        ) then online_Source
        when kyc_type = 'Referral' then referal_type
        when kyc_type = 'DRA' THEN final_category
        when kyc_type = 'Other' THEN 'Other'
      end as L2_tags,
          age,
          kyc_type,
          diy,
          diy_type,
          tradedwithindays,
          traded_bucket,
          totaltradevalue,
          firsttradevalue,
          totaltradevalue_thirtydays,
          countoftradebyclient as Total_trades,
          countoftradebyclient_thirtydays as Total_trades_thirtydays,
          brokerage_thirtydays,
          row_number() over(
            partition by party_code
            order by
              dt desc
          ) rid
        from
          dbo_sales_bi.tb_kyc
        where
          party_code is not null
      )
    where
      rid = 1
  ),
  
  Order_count as(with rs as (select party_code,oc,t_o,brokerage,sauda_date,inst_type
            from "dbo-angels"."db_online_engine_rev"."as_ordercountdata" where sauda_date>=current_date-91)
SELECT party_code as client_id,
sum(case when inst_type in('Del') and sauda_date>=current_date-90 then oc end)as delivery_OC_Last90Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-90 then oc end)as FNO_OC_Last90Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-90 then oc end)as Intraday_OC_Last90Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-90 then oc end)as Commodity_OC_Last90Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-90 then oc end)as Currency_OC_Last90Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-60 then oc end)as delivery_OC_Last60Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-60 then oc end)as FNO_OC_Last60Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-60 then oc end)as Intraday_OC_Last60Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-60 then oc end)as Commodity_OC_Last60Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-60 then oc end)as Currency_OC_Last60Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-30 then oc end)as delivery_OC_Last30Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-30 then oc end)as FNO_OC_Last30Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-30 then oc end)as Intraday_OC_Last30Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-30 then oc end)as Commodity_OC_Last30Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-30 then oc end)as Currency_OC_Last30Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-90 then t_o end)as delivery_TurnOver_Last90Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-90 then t_o end)as FNO_TurnOver_Last90Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-90 then t_o end)as Intraday_TurnOver_Last90Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-90 then t_o end)as Commodity_TurnOver_Last90Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-90 then t_o end)as Currency_TurnOver_Last90Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-60 then t_o end)as delivery_TurnOver_Last60Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-60 then t_o end)as FNO_TurnOver_Last60Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-60 then t_o end)as Intraday_TurnOver_Last60Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-60 then t_o end)as Commodity_TurnOver_Last60Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-60 then t_o end)as Currency_TurnOver_Last60Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-30 then t_o end)as delivery_TurnOver_Last30Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-30 then t_o end)as FNO_TurnOver_Last30Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-30 then t_o end)as Intraday_TurnOver_Last30Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-30 then t_o end)as Commodity_TurnOver_Last30Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-30 then t_o end)as Currency_TurnOver_Last30Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-90 then brokerage end)as delivery_Brokerage_Last90Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-90 then brokerage end)as FNO_Brokerage_Last90Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-90 then brokerage end)as Intraday_Brokerage_Last90Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-90 then brokerage end)as Commodity_Brokerage_Last90Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-90 then brokerage end)as Currency_Brokerage_Last90Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-60 then brokerage end)as delivery_Brokerage_Last60Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-60 then brokerage end)as FNO_Brokerage_Last60Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-60 then brokerage end)as Intraday_Brokerage_Last60Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-60 then brokerage end)as Commodity_Brokerage_Last60Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-60 then brokerage end)as Currency_Brokerage_Last60Days,

sum(case when inst_type in('Del') and sauda_date>=current_date-30 then brokerage end)as delivery_Brokerage_Last30Days,
sum(case when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK') and sauda_date>=current_date-30 then brokerage end)as FNO_Brokerage_Last30Days,
sum(case when inst_type in('Intr') and sauda_date>=current_date-30 then brokerage end)as Intraday_Brokerage_Last30Days,
sum(case when inst_type in('COMM') and sauda_date>=current_date-30 then brokerage end)as Commodity_Brokerage_Last30Days,
sum(case when inst_type in('Currency') and sauda_date>=current_date-30 then brokerage end)as Currency_Brokerage_Last30Days

FROM rs
group by 1)
,


  KYC as (
    select
      party_code as client_id,
    case when b2c='Y' then 'B2C' 
    when b2c='N' then 'B2B' end as B2C_tag,
      activefrom,
      partycode_gendate,
      inactivefrom as ClientInActiveFrom,
      activefromeq as ActiveOnEq,
      inactivefromeq,
      activefromcomm as ActiveOnComm,
      inactivefromcomm,
      activefromcurr as ActiveOnCurr,
      inactivefromcurr,
      activefromfo as ActiveOnFnO,
      inactivefromfo,
      firsttrade as DateFirstTrade,
      lasttrade as DateLastTrade,
      firstledgerdate as DateFirstFundAdded,
      firsttradeeq as DateFirstTradeEquity,
      lasttradeeq as DateLastTradeEquity,
      firsttradecomm as DateFirstTradeComm,
      lasttradecomm as DateLastTradeComm,
      firsttradecurr as DateFirstTradeCurr,
      lasttradecurr as DateLastTradeCurr,
    lower(city) as city_kyc,
    lower(state) as state_kyc,
    lower(nation)as country_kyc,
      case when scheme_name is null then 'Old'
         when scheme_name in ('I TRADE PRIME') then 'iTradePrime' else 'iTrade' end as scheme_name
    from
      db_online_engine_rev.sn_clientkyc
  ),

  order30days as(
    SELECT
      client_id,
      sum(cast(cash as int)) as TotalEqOrderCountLast30days,
      sum(cast(fno as int)) as TotalFnOOrderCount30days,
      sum(cast(currency as int)) as TotalCurrOrderCount30days,
      sum(cast(commodity as int)) as TotalCommOrderOrderCount30days
    FROM
      dbo_order_s3.overall_order_submit_raw
    where
      dt >= current_date -30
    group by
      1
  ),
  order60days as(
    SELECT
      client_id,
      sum(cast(cash as int)) as TotalEqOrderCountLast60days,
      sum(cast(fno as int)) as TotalFnOOrderCount60days,
      sum(cast(currency as int)) as TotalCurrOrderCount60days,
      sum(cast(commodity as int)) as TotalCommOrderOrderCount60days
    FROM
      dbo_order_s3.overall_order_submit_raw
    where
      dt >= current_date -60
    group by
      1
  ),
  order90days as(
    SELECT
      client_id,
      sum(cast(cash as int)) as TotalEqOrderCountLast90days,
      sum(cast(fno as int)) as TotalFnOOrderCount90days,
      sum(cast(currency as int)) as TotalCurrOrderCount90days,
      sum(cast(commodity as int)) as TotalCommOrderOrderCount90days
    FROM
      dbo_order_s3.overall_order_submit_raw
    where
      dt >= current_date -90
    group by
      1
  ),
  IPO_dates as (
    SELECT
      user_id as client_id,
      min(cast(applicationdatetime as date)) DateFirstIPOApplied,
      max(cast(applicationdatetime as date)) DateLastIPOApplied
    FROM
      dbo_ipo_s3.admin_asba
    group by
      1
  ),
  TotalIPOs as (
    SELECT
      user_id as client_id,
      count(distinct(iponame)) TotaluniqueIPOapplied1yr,
      count((iponame)) TotalIPOapplied1yr
    FROM
      dbo_ipo_s3.admin_asba
    where
      cast(applicationdatetime as date) >= date_add('day', -365, current_date)
    group by
      1
  ),
  KYC_fact as(
    select
      *
    from
      (
        SELECT
          party_code as client_id,
          mobile,
          lower(email_id) email_id,
          b2b_b2c as ClientType,
          row_number() over(
            partition by party_code
            order by
              updatedat desc
          ) rid
        FROM
          dbo_kycfulfillment.kyc_data
      )
    where
      rid = 1
  ),
  LoginUsers as (
    SELECT
      distinct client_id
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and dt >= current_date -90
  ),
  ABMAAndroid as (
    SELECT
      client_id,
      max(dt) as ABMAAndroid_last_login
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and length(client_id) > 1
      and concat(application, device) = 'ABMAAndroid'
    group by
      1
  ),
  ABMAIOS as (
    SELECT
      client_id,
      max(dt) as ABMAIOS_last_login
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and length(client_id) > 1
      and concat(application, device) = 'ABMAIOS'
    group by
      1
  ),
  SPARKIOS as (
    SELECT
      client_id,
      max(dt) as SPARKIOS_last_login
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and length(client_id) > 1
      and concat(application, device) = 'SPARKIOS'
    group by
      1
  ),
  SPARKAndroid as (
    SELECT
      client_id,
      max(dt) as SPARKAndroid_last_login
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and length(client_id) > 1
      and concat(application, device) = 'SPARKAndroid'
    group by
      1
  ),
  TAB as (
    SELECT
      client_id,
      max(dt) as TAB_last_login
    FROM
      dbo_login.overall_login_daily_agg_metrics_users
    where
      success_count > 0
      and length(client_id) > 1
      and concat(application, device) = 'TAB-'
    group by
      1
  ),
lastFund as (select
          client_code as client_id,
          cast(max(transreq_dtm) as date) as DateLastFundAdded
        from
          dbo_auth.Pg_Transaction
        where
          status = 'SUCCESS'
          
        group by
          1), 
AppsFlyer as (select * from (select distinct("client id") as client_id,"country code" as country_appsFlyer,state as state_appsFlyer,city as city_appsFlyer,operator,wifi, 
row_number() over(
            partition by "client id"
            order by
              "event time" desc
          ) rid
 from "dbo-angels"."dbo_appsflyer"."appsflyer_final" where "event name"='login_success'
 order by 1)
 where rid=1),
            
NPS as(with aa as
(
select Replace(clientcode,' ','') as clientcode,Surveymasterid,
  case when cast(rating as varchar)in ('0','1','2','3','4','5','6') then 'detractor'
   when cast(rating as varchar)in ('7','8') then 'Passive'
    when  cast(rating as varchar) in ('9','10') then 'promoter'  end as type
from dbo_nps.surveyclientleadsanswer
where clientcode not in  ('K180192','H78036','B157991','B61555','S630660','J142863')
  ),
 bb as
(
select surveyname,id from dbo_nps.surveymaster
 )
select distinct a.clientcode as client_id,
count(client_id) as Total_NPS_Response,
count(case when a.type in ('detractor') then 'detractor' end) as detractor,
count(case when a.type in ('promoter') then 'detractor' end) as promoter,

count(case when surveyname in ('Portfolio') then client_id end) as Total_NPS_Response_Portfolio,
count(case when a.type in ('detractor') and  surveyname in ('Portfolio') then 'detractor' end) as detractor_Portfolio,
count(case when a.type in ('promoter') and  surveyname in ('Portfolio') then 'detractor' end) as promoter_Portfolio,

count(case when surveyname in ('IPO') then client_id end) as Total_NPS_Response_IPO,
count(case when a.type in ('detractor') and  surveyname in ('IPO') then 'detractor' end) as detractor_IPO,
count(case when a.type in ('promoter') and  surveyname in ('IPO') then 'detractor' end) as promoter_IPO,

count(case when surveyname in ('Charts') then client_id end) as Total_NPS_Response_Charts,
count(case when a.type in ('detractor') and  surveyname in ('Charts') then 'detractor' end) as detractor_Charts,
count(case when a.type in ('promoter') and  surveyname in ('Charts') then 'detractor' end) as promoter_Charts,

count(case when surveyname in ('Add Funds') then client_id end) as Total_NPS_Response_AddFunds
,
count(case when a.type in ('detractor') and  surveyname in ('Add Funds') then 'detractor' end) as detractor_AddFunds,
count(case when a.type in ('promoter') and  surveyname in ('Add Funds') then 'detractor' end) as promoter_AddFunds,

count(case when surveyname in ('Orders') then client_id end) as Total_NPS_Response_Orders,
count(case when a.type in ('detractor') and  surveyname in ('Orders') then 'detractor' end) as detractor_Orders,
count(case when a.type in ('promoter') and  surveyname in ('Orders') then 'detractor' end) as promoter_Orders,

count(case when surveyname in ('Sales_cm_onboarding') then client_id end) as Total_NPS_Response_KYC,
count(case when a.type in ('detractor') and  surveyname in ('Sales_cm_onboarding') then 'detractor' end) as detractor_KYC,
count(case when a.type in ('promoter') and  surveyname in ('Sales_cm_onboarding') then 'detractor' end) as promoter_KYC


 from aa a
 inner  join bb b on a.Surveymasterid =b.id
 group by 1
 order by 2 desc),  
            

            portfolio as (with times as (select max(lastupdatetime)lastupdatetime from dbo_portfolio_live.portfolio_live_bg
where CAST(dt as date)>=date_add('day',-1,current_date)),

rs as (select lastupdatetime,partycode,coname,sector,(cast(avgprice as decimal)*cast(angelqty as decimal)) as investment_value,(cast(lasttradeprice as decimal)*cast(angelqty as decimal)) as protfolio_value from  dbo_portfolio_live.portfolio_live_bg where CAST(dt as date)>=date_add('day',-1,current_date)),
final as(select a.* from rs a inner join times as b on a.lastupdatetime=b.lastupdatetime)
select partycode,count(distinct(coname))as Total_scrips,sum(investment_value)investment_value,sum(protfolio_value)protfolio_value from final
group by 1),

/*
            bonds as(select distinct(client_code) as client_id,count(1) as Total_Bond_Orders,max(order_date)Last_Bond_Order,
min(order_date)First_Bond_Order from dbo_bond.Vw_Bond_Detail
group by 1), 
*/                     
            
  add_funds as (
    select
      client_code,
      sum(
        case
          when date_diff(
            'day',
            cast((transreq_dtm) as date),
            current_date
          ) <= 30 then (amount)
          else 0
        end
      ) AddFund_Last30_days,
      sum(
        case
          when date_diff(
            'day',
            cast((transreq_dtm) as date),
            current_date
          ) <= 60 then (amount)
          else 0
        end
      ) AddFund_Last60_days,
      sum(
        case
          when date_diff(
            'day',
            cast((transreq_dtm) as date),
            current_date
          ) <= 90 then (amount)
          else 0
        end
      ) AddFund_Last90_days
    from
      (
        select
          client_code,
          transreq_dtm,
          sum(amount) amount
        from
          dbo_auth.Pg_Transaction
        where
          status = 'SUCCESS'
          and cast(
            date(cast(transreq_dtm as timestamp)) as varchar
          ) >= date_add('day', -91, current_date)
        group by
          client_code,
          transreq_dtm
      )
    group by
      client_code
  )       
select
  a.client_id,
  case when b2c_dra_tag='other' then B2C_tag else b2c_dra_tag end as ClientType,
  Application_no,
  cast(partycode_gendate as date) partycode_gendate, 
  cast(activefrom as date) activefrom,
  email_id as email_id,
  mobile as mobile,
  clientname,
  age,
  case when age<18 then 'Less than 18'
when age>=18 and age<=25 then '18-25'
when age>=26 and age<=30 then '26-30'
when age>=31 and age<=35 then '31-35'
when age>=36 and age<=40 then '36-40'
when age>=41 and age<=45 then '41-45'
when age>=46 and age<=50 then '46-50'
when age>=51 then '50+' end as age_bucket,
city_kyc,   
city_appsFlyer,
state_kyc,
state_appsFlyer,
country_kyc,  
country_appsFlyer,        
operator,
wifi,        
  L1_tags,
  L2_tags,
  kyc_type,
  diy,
  diy_type,
  Total_scrips as EQ_Portfolio_Total_Scrips  ,
  investment_value as       EQ_Portfolio_Investment_Value,
  protfolio_value as EQ_Portfolio_Current_Value,

  cast(ClientInActiveFrom as date) ClientInActiveFrom,
  cast(ActiveOnEq as date) ActiveOnEq,
  cast(inactivefromeq as date) inactivefromeq,
  cast(ActiveOnComm as date) ActiveOnComm,
  cast(inactivefromcomm as date) inactivefromcomm,
  cast(ActiveOnCurr as date) ActiveOnCurr,
  cast(inactivefromcurr as date) inactivefromcurr,
  cast(ActiveOnFnO as date) ActiveOnFnO,
  cast(inactivefromfo as date) inactivefromfo,
  cast(DateFirstFundAdded as date) DateFirstFundAdded,
  cast(DateLastFundAdded as date) as DateLastFundAdded,
  cast('01-01-9999' as date) as DateLastFundWithdraw,
  AddFund_Last30_days,
  AddFund_Last60_days,
  AddFund_Last90_days,
  cast(DateFirstTrade as date) DateFirstTrade,
  cast(DateLastTrade as date) DateLastTrade,
  tradedwithindays,
  traded_bucket,
  totaltradevalue,
  firsttradevalue,
  delivery_OC_Last30Days,
   delivery_OC_Last60Days,
   delivery_OC_Last90Days,
   FNO_OC_Last30Days,     
   FNO_OC_Last60Days,     
   FNO_OC_Last90Days,     
Intraday_OC_Last30Days,
Intraday_OC_Last60Days,
Intraday_OC_Last90Days,
Commodity_OC_Last30Days,
Commodity_OC_Last60Days,
Commodity_OC_Last90Days,
Currency_OC_Last30Days,
Currency_OC_Last60Days,
Currency_OC_Last90Days,    
  delivery_TurnOver_Last30Days,
   delivery_TurnOver_Last60Days,
   delivery_TurnOver_Last90Days,
   FNO_TurnOver_Last30Days,     
   FNO_TurnOver_Last60Days,     
   FNO_TurnOver_Last90Days,     
Intraday_TurnOver_Last30Days,
Intraday_TurnOver_Last60Days,
Intraday_TurnOver_Last90Days,
Commodity_TurnOver_Last30Days,
Commodity_TurnOver_Last60Days,
Commodity_TurnOver_Last90Days,
Currency_TurnOver_Last30Days,
Currency_TurnOver_Last60Days,
Currency_TurnOver_Last90Days, 
  delivery_Brokerage_Last30Days,
   delivery_Brokerage_Last60Days,
   delivery_Brokerage_Last90Days,
   FNO_Brokerage_Last30Days,     
   FNO_Brokerage_Last60Days,     
   FNO_Brokerage_Last90Days,     
Intraday_Brokerage_Last30Days,
Intraday_Brokerage_Last60Days,
Intraday_Brokerage_Last90Days,
Commodity_Brokerage_Last30Days,
Commodity_Brokerage_Last60Days,
Commodity_Brokerage_Last90Days,
Currency_Brokerage_Last30Days,
Currency_Brokerage_Last60Days,
Currency_Brokerage_Last90Days,         
        cast(DateFirstTradeEquity as date) DateFirstTradeEquity,
  cast(DateLastTradeEquity as date) DateLastTradeEquity,
  cast('01-01-9999' as date)  DateFirstTradeFnO,
  cast('01-01-9999' as date)  DateLastTradeFnO,
  cast(DateFirstTradeCurr as date) DateFirstTradeCurr,
  cast(DateLastTradeCurr as date) DateLastTradeCurr,
  cast(DateFirstTradeComm as date) DateFirstTradeComm,
  cast(DateLastTradeComm as date) DateLastTradeComm,
  0 as TotalMFOrderCount,
  0 as MFPortfolioValue,
  cast('01-01-9999' as date) as DateFirstTradeMF,
  cast('01-01-9999' as date) as DateLastTradeMF,
 -- Total_Bond_Orders as TotalBondsOrderCount,
 -- First_Bond_Order as DateFirstTradeBonds,
 -- Last_Bond_Order as DateLastTradeBonds,
  TotaluniqueIPOapplied1yr,
  TotalIPOapplied1yr,
  cast(DateFirstIPOApplied as date) as DateFirstIPOApplied,
  cast(DateLastIPOApplied as date) as DateLastIPOApplied,
  0 as TotalGTTordercount,
  cast('01-01-9999' as date) as DateFirstGTTOrder,
  cast('01-01-9999' as date) as DateLastGTTOrder,
  0 as StockSIPOrdercount,
  cast('01-01-9999' as date) as DateFirstSIPOrder,
  cast('01-01-9999' as date) as DateLastSIPOrder,
total_nps_response,
detractor,
promoter,
total_nps_response_portfolio,
total_nps_response_ipo,
total_nps_response_charts,
total_nps_response_addfunds,
total_nps_response_orders,
total_nps_response_kyc,

        
  cast(ABMAAndroid_last_login as date) ABMAAndroid_last_login,
  cast(ABMAIOS_last_login as date) ABMAIOS_last_login,
  cast(SPARKAndroid_last_login as date) SPARKAndroid_last_login,
  cast(SPARKIOS_last_login as date) SPARKIOS_last_login,
  cast(TAB_last_login as date) TAB_last_login
from
  LoginUsers as a
  left join Master b on a.client_id = b.client_id
  inner join KYC c on a.client_id = c.client_id
  left join IPO_dates g on a.client_id = g.client_id
  left join TotalIPOs h on a.client_id = h.client_id
  left join KYC_fact i on a.client_id = i.client_id
  left join ABMAAndroid j on a.client_id = j.client_id
  left join ABMAIOS k on a.client_id = k.client_id
  left join SPARKIOS l on a.client_id = l.client_id
  left join SPARKAndroid m on a.client_id = m.client_id
  left join TAB n on a.client_id = n.client_id
  left join add_funds o on a.client_id = o.client_code 
  left join lastFund p on a.client_id = p.client_id     
  left join Order_count q on a.client_id = q.client_id  
  left join NPS r on a.client_id = r.client_id  
  left join AppsFlyer s on a.client_id = s.client_id
  left join portfolio t on a.client_id = t.partycode;
begin transaction;
delete from product_analytics.client_fact
where  (nvl(ABMAAndroid_last_login,'1900-01-01')<=current_date-90
and    nvl(ABMAIOS_last_login,'1900-01-01')<=current_date-90
and    nvl(SPARKAndroid_last_login,'1900-01-01') <=current_date-90
and   nvl(SPARKIOS_last_login,'1900-01-01')<=current_date-90
and  nvl(TAB_last_login,'1900-01-01')<=current_date-90);
delete from
  product_analytics.client_fact using client_fact_stg
where
  client_fact.client_id = client_fact_stg.client_id;
insert into
  product_analytics.client_fact
select
  *
from
  client_fact_stg;
end transaction;
drop table client_fact_stg;