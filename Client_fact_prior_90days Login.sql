 with LoginUsers as( 
    select * from (
SELECT distinct client_id,
case when 
last_activity_abma_android>=last_activity_abma_ios and last_activity_abma_android>=last_activity_spark_ios and last_activity_abma_android>=last_activity_spark_android and last_activity_abma_android>=last_activity_spark_web and last_activity_abma_android>=last_activity_tab_web then last_activity_abma_android

when 
last_activity_abma_ios>=last_activity_abma_android and last_activity_abma_ios>=last_activity_spark_ios and last_activity_abma_ios>=last_activity_spark_android and last_activity_abma_ios>=last_activity_spark_web and last_activity_abma_ios>=last_activity_tab_web then last_activity_abma_ios

when 
last_activity_spark_ios>=last_activity_abma_android and last_activity_spark_ios>=last_activity_abma_ios and last_activity_spark_ios>=last_activity_spark_android and last_activity_spark_ios>=last_activity_spark_web and last_activity_spark_ios>=last_activity_tab_web then last_activity_spark_ios

when 
last_activity_spark_android>=last_activity_abma_android and last_activity_spark_android>=last_activity_abma_ios and last_activity_spark_android>=last_activity_spark_ios and last_activity_spark_android>=last_activity_spark_web and last_activity_spark_android>=last_activity_tab_web then last_activity_spark_android

when 
last_activity_spark_web>=last_activity_abma_android and last_activity_spark_web>=last_activity_abma_ios and last_activity_spark_web>=last_activity_spark_ios and last_activity_spark_web>=last_activity_spark_android and last_activity_spark_web>=last_activity_tab_web then last_activity_spark_web

when 
last_activity_tab_web>=last_activity_abma_android and last_activity_tab_web>=last_activity_abma_ios and last_activity_tab_web>=last_activity_spark_ios and last_activity_tab_web>=last_activity_spark_android and last_activity_tab_web>=last_activity_spark_web then last_activity_tab_web end as last_login

from (
 Select client_id,
 case when last_activity_abma_android is null then '1990-01-01'  else last_activity_abma_android end as last_activity_abma_android,
 case when last_activity_abma_ios is null then '1990-01-01' else last_activity_abma_ios end as last_activity_abma_ios,
 case when last_activity_spark_android is null then '1990-01-01'  else last_activity_spark_android end as last_activity_spark_android,
 case when last_activity_spark_ios is null then '1990-01-01' else last_activity_spark_ios end as last_activity_spark_ios,
 case when last_activity_spark_web is null then '1990-01-01' else last_activity_spark_web end as last_activity_spark_web,
 case when last_activity_tab_web is null then '1990-01-01' else last_activity_tab_web end as last_activity_tab_web

 FROM "dbo-angels"."product_analytics"."last_activity"
 
 
))
 where last_login >= current_date -365

),


-- LoginUsersv2 as 
-- (select * from 
-- (SELECT distinct client_id,max(dt) as last_login_date
--   FROM
--     dbo_login.overall_login_daily_agg_metrics_users
--   where
--     success_count > 0
--     and dt >= current_date -365
--   group by
--     1)
--     where last_login<current_date-90
-- ),
Master as(
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
          when kyc_type = 'DRA' then 'DRA'
          else 'other'
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
Order_count as(
  with rs1 as (
    select
      party_code,
      oc,
      t_o,
      brokerage,
      sauda_date,
      inst_type
    from
      "dbo-angels"."db_online_engine_rev"."as_ordercountdata"

    union all
    select
      party_code,
      oc,
      t_o,
      brokerage,
      sauda_date,
      inst_type
    from
      "dbo-angels"."db_online_engine_rev"."up_b2b_ordercountdata"

  ),
  login as (
    select
      distinct client_id,
      last_login
    from
      LoginUsers
  ),
  rs as (
    select
      party_code,
      sauda_date,
      inst_type,
      sum(oc) as oc,
      sum(t_o) as t_o,
      sum(brokerage) brokerage
    from
      rs1
    group by
      1,
      2,
      3
  )
  SELECT
    party_code as client_id,
sum(oc) as Total_orders_placed,
sum(brokerage) as total_gross_brokerage_ltv,
    avg(
      case
        when sauda_date >= last_login -91 then oc
      end
    ) as Daily_avg_last90_days,
    avg(
      case
        when sauda_date >= last_login -61 then oc
      end
    ) as Daily_avg_last60_days,
    avg(
      case
        when sauda_date >= last_login -31 then oc
      end
    ) as Daily_avg_last30_days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -90 then oc
      end
    ) as delivery_OC_Last90Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -90 then oc
      end
    ) as FNO_OC_Last90Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -90 then oc
      end
    ) as Intraday_OC_Last90Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -90 then oc
      end
    ) as Commodity_OC_Last90Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -90 then oc
      end
    ) as Currency_OC_Last90Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -60 then oc
      end
    ) as delivery_OC_Last60Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -60 then oc
      end
    ) as FNO_OC_Last60Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -60 then oc
      end
    ) as Intraday_OC_Last60Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -60 then oc
      end
    ) as Commodity_OC_Last60Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -60 then oc
      end
    ) as Currency_OC_Last60Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -30 then oc
      end
    ) as delivery_OC_Last30Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -30 then oc
      end
    ) as FNO_OC_Last30Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -30 then oc
      end
    ) as Intraday_OC_Last30Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -30 then oc
      end
    ) as Commodity_OC_Last30Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -30 then oc
      end
    ) as Currency_OC_Last30Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -90 then t_o
      end
    ) as delivery_TurnOver_Last90Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -90 then t_o
      end
    ) as FNO_TurnOver_Last90Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -90 then t_o
      end
    ) as Intraday_TurnOver_Last90Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -90 then t_o
      end
    ) as Commodity_TurnOver_Last90Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -90 then t_o
      end
    ) as Currency_TurnOver_Last90Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -60 then t_o
      end
    ) as delivery_TurnOver_Last60Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -60 then t_o
      end
    ) as FNO_TurnOver_Last60Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -60 then t_o
      end
    ) as Intraday_TurnOver_Last60Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -60 then t_o
      end
    ) as Commodity_TurnOver_Last60Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -60 then t_o
      end
    ) as Currency_TurnOver_Last60Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -30 then t_o
      end
    ) as delivery_TurnOver_Last30Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -30 then t_o
      end
    ) as FNO_TurnOver_Last30Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -30 then t_o
      end
    ) as Intraday_TurnOver_Last30Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -30 then t_o
      end
    ) as Commodity_TurnOver_Last30Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -30 then t_o
      end
    ) as Currency_TurnOver_Last30Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -90 then brokerage
      end
    ) as delivery_Brokerage_Last90Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -90 then brokerage
      end
    ) as FNO_Brokerage_Last90Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -90 then brokerage
      end
    ) as Intraday_Brokerage_Last90Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -90 then brokerage
      end
    ) as Commodity_Brokerage_Last90Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -90 then brokerage
      end
    ) as Currency_Brokerage_Last90Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -60 then brokerage
      end
    ) as delivery_Brokerage_Last60Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -60 then brokerage
      end
    ) as FNO_Brokerage_Last60Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -60 then brokerage
      end
    ) as Intraday_Brokerage_Last60Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= current_date -60 then brokerage
      end
    ) as Commodity_Brokerage_Last60Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -60 then brokerage
      end
    ) as Currency_Brokerage_Last60Days,
    sum(
      case
        when inst_type in('Del')
        and sauda_date >= last_login -30 then brokerage
      end
    ) as delivery_Brokerage_Last30Days,
    sum(
      case
        when inst_type in('FUTSTK', 'FUTIDX', 'OPTIDX', 'OPTSTK')
        and sauda_date >= last_login -30 then brokerage
      end
    ) as FNO_Brokerage_Last30Days,
    sum(
      case
        when inst_type in('Intr')
        and sauda_date >= last_login -30 then brokerage
      end
    ) as Intraday_Brokerage_Last30Days,
    sum(
      case
        when inst_type in('COMM')
        and sauda_date >= last_login -30 then brokerage
      end
    ) as Commodity_Brokerage_Last30Days,
    sum(
      case
        when inst_type in('Currency')
        and sauda_date >= last_login -30 then brokerage
      end
    ) as Currency_Brokerage_Last30Days
  FROM
    rs a
    inner join login as b on a.party_code = b.client_id
  group by
    1
),
KYC as (
  select
    distinct party_code as client_id,
    datediff('year', cast(birthdate as date), current_date) as age,
    case
      when b2c = 'Y' then 'B2C'
      when b2c = 'N' then 'B2B'
    end as B2C_tag,
    activefrom,
    sub_broker,
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
    birthdate as birthdate,
    bsecmactivefrom as Date_Active_BSE,
    bsecminactivefrom as Date_Inactive_BSE,
    nsecmactivefrom as Date_Active_NSE,
    nsecminactivefrom as Date_Inactive_NSE,
    lower(city) as city_kyc,
    lower(state) as state_kyc,
    lower(nation) as country_kyc,
    case
      when scheme_name is null then 'Old'
      when scheme_name in ('I TRADE PRIME') then 'iTradePrime'
      else 'iTrade'
    end as scheme_name
  from
    db_online_engine_rev.sn_clientkyc
),
ipo_dates as (
  select
    t1.client_code as client_id,
    min(cast(bid_datetime as date)) datefirstipoapplied,
    max(cast(bid_datetime as date)) datelastipoapplied
  from
    dbo_ipo_s3.ipov2_public_orders t1
    inner join (
      SELECT
        max(last_updated) last_updated,
        client_code,
        id
      FROM
        dbo_ipo_s3.ipov2_public_orders t1
      group by
        client_code,
        id
    ) t4 on t4.client_code = t1.client_code
    and t4.id = t1.id
    and cast(t4.last_updated as date) = cast(t1.last_updated as date)
  where
    transaction_type = 7021
    and (
      exchange is not null
      and exchange <> ''
    )
  group by
    1
),
totalipos1 as (
  select
    t1.client_code,
    count(distinct name) totaluniqueipoapplied1yr,
    count(distinct t1.id) totalipoapplied1yr
  from
    dbo_ipo_s3.ipov2_public_orders t1
    inner join (
      SELECT
        max(last_updated) last_updated,
        client_code,
        id
      FROM
        dbo_ipo_s3.ipov2_public_orders t1
      group by
        client_code,
        id
    ) t4 on t4.client_code = t1.client_code
    and t4.id = t1.id
    and cast(t4.last_updated as date) = cast(t1.last_updated as date)
    inner join dbo_ipo_s3.ipov2_public_ipos t2 on t2.id = t1.ipo_id
  where
    cast(bid_datetime as date) >= dateadd('day', -365, current_date)
    and transaction_type = 7021
    and (
      exchange is not null
      and exchange <> ''
    )
  group by
    1
),
totalipos as (
  select
    distinct t1.client_id as client_id,
    t1.datefirstipoapplied,
    t1.datelastipoapplied,
    t2.totaluniqueipoapplied1yr,
    t2.totalipoapplied1yr
  from
    ipo_dates t1
    inner join totalipos1 t2 on t2.client_code = t1.client_id
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
Sparkweb as (
  SELECT
    client_id,
    max(dt) as Sparkweb_last_login
  FROM
    dbo_login.overall_login_daily_agg_metrics_users
  where
    success_count > 0
    and length(client_id) > 1
    and concat(application, device) = 'WEBWEB'
  group by
    1
),
lastFund as (
  select
    client_code as client_id,
    cast(max(transreq_dtm) as date) as DateLastFundAdded
  from
    dbo_auth.Pg_Transaction
  where
    status = 'SUCCESS'
  group by
    1
),
AppsFlyer as (
  select
    Client_id,
    AppsFlyer_AUC,
    Mobile_No,
    fullname,
    Device_make as Device_make_appsFlyer,
    Device_Model as Device_Model_appsFlyer,
    cast(install_time as timestamp) as install_time_appsFlyer,
    cast(login_time as timestamp) as Lastlogin_appsFlyer,
    platform as platform_appsFlyer,
    app_version as app_versionappsFlyer,
    operator,
    wifi,
    country_appsFlyer,
    state_appsFlyer,
    city_appsFlyer,
    cast(Uninstall_time as timestamp) as Uninstall_time_appsFlyer
  from
    "dbo-angels"."dbo_appsflyer"."Summary_Login"
),
NPS as(
  with aa as (
    select
      Replace(clientcode, ' ', '') as clientcode,
      Surveymasterid,
      case
        when cast(rating as varchar) in ('0', '1', '2', '3', '4', '5', '6') then 'detractor'
        when cast(rating as varchar) in ('7', '8') then 'Passive'
        when cast(rating as varchar) in ('9', '10') then 'promoter'
      end as type
    from
      dbo_nps.surveyclientleadsanswer
    where
      clientcode not in (
        'K180192',
        'H78036',
        'B157991',
        'B61555',
        'S630660',
        'J142863'
      )
      and updatedat < '2022-09-01'
  ),
  bb as (
    select
      surveyname,
      id
    from
      dbo_nps.surveymaster
  )
  select
    distinct a.clientcode as client_id,
    count(client_id) as Total_NPS_Response,
    count(
      case
        when a.type in ('detractor') then 'detractor'
      end
    ) as detractor,
    count(
      case
        when a.type in ('promoter') then 'detractor'
      end
    ) as promoter,
    count(
      case
        when surveyname in ('Portfolio') then client_id
      end
    ) as Total_NPS_Response_Portfolio,
    count(
      case
        when surveyname in ('IPO') then client_id
      end
    ) as Total_NPS_Response_IPO,
    count(
      case
        when surveyname in ('Charts') then client_id
      end
    ) as Total_NPS_Response_Charts,
    count(
      case
        when surveyname in ('Add Funds') then client_id
      end
    ) as Total_NPS_Response_AddFunds,
    count(
      case
        when surveyname in ('Orders') then client_id
      end
    ) as Total_NPS_Response_Orders,
    count(
      case
        when surveyname in ('Sales_cm_onboarding') then client_id
      end
    ) as total_nps_response_kyc,
    count(
      case
        when surveyname in ('Watchlist') then client_id
      end
    ) as Total_nps_response_watchlist,
    count(
      case
        when surveyname in ('Homepage') then client_id
      end
    ) as Total_nps_response_homepage,
    count(
      case
        when surveyname in ('Reports') then client_id
      end
    ) as Total_nps_response_reports,
    count(
      case
        when surveyname in ('InstaTrade') then client_id
      end
    ) as Total_nps_response_insta_trade,
    count(
      case
        when surveyname in ('FBO') then client_id
      end
    ) as Total_nps_response_first_buy_order,
    count(
      case
        when surveyname in ('KYC2') then client_id
      end
    ) as Total_nps_response_KYC2,
    count(
      case
        when surveyname in ('MTF') then client_id
      end
    ) as Total_nps_response_MTF
  from
    aa a
    inner join bb b on a.Surveymasterid = b.id
  group by
    1
  union all
  SELECT
    clientcode as client_id,
    count(clientcode) as total_nps_response,
    SUM(
      CASE
        WHEN segment = 'Detractors' THEN 1
        ELSE 0
      END
    ) AS Detractors,
    SUM(
      CASE
        WHEN segment = 'Promoters' THEN 1
        ELSE 0
      END
    ) AS Promoters,
    SUM(
      CASE
        WHEN surveyname like '%Portfolio%' THEN 1
        ELSE 0
      END
    ) AS Total_portfolio_Response,
    SUM(
      CASE
        WHEN surveyname like '%IPO%' THEN 1
        ELSE 0
      END
    ) AS Total_ipo_Response,
    SUM(
      CASE
        WHEN surveyname like '%harts%' THEN 1
        ELSE 0
      END
    ) AS Total_charts_Response,
    SUM(
      CASE
        WHEN surveyname like '%Add Fund%' THEN 1
        ELSE 0
      END
    ) AS Total_add_funds_Response,
    SUM(
      CASE
        WHEN surveyname like '%Orders%' THEN 1
        ELSE 0
      END
    ) AS Total_Order_Response,
    SUM(
      CASE
        WHEN surveyname like 'Sales_cm_onboarding%' THEN 1
        ELSE 0
      END
    ) AS total_nps_response_kyc,
    SUM(
      CASE
        WHEN surveyname like '%Watchlist%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_watchlist,
    SUM(
      CASE
        WHEN surveyname like '%Homepage%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_homepage,
    SUM(
      CASE
        WHEN surveyname like '%Reports%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_reports,
    SUM(
      CASE
        WHEN surveyname like '%InstaTrade%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_insta_trade,
    SUM(
      CASE
        WHEN surveyname like '%FBO%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_first_buy_order,
    SUM(
      CASE
        WHEN surveyname like '%KYC2%'
        or surveyname like '%Sales - Non Assisted%' THEN 1
        ELSE 0
      END
    ) AS Total_nps_response_KYC2,
    SUM(
      CASE
        WHEN surveyname like 'MTF' THEN 1
        ELSE 0
      END
    ) AS Total_MTF_nps_respons
  FROM
    "dbo-angels"."dbo_nps_internal"."nps_info_stg"
  group by
    clientcode
),
portfolio as (
  with times as (
    select
      max(lastupdatetime) lastupdatetime
    from
      dbo_portfolio_live.portfolio_live_bg
    where
      CAST(dt as date) >= date_add('day', -1, current_date)
  ),
  rs as (
    select
      lastupdatetime,
      partycode,
      coname,
      sector,
      (
        cast(avgprice as decimal) * cast(angelqty as decimal)
      ) as investment_value,
      (
        cast(lasttradeprice as decimal) * cast(angelqty as decimal)
      ) as protfolio_value
    from
      dbo_portfolio_live.portfolio_live_bg
    where
      CAST(dt as date) >= date_add('day', -1, current_date)
  ),
  final as(
    select
      a.*
    from
      rs a
      inner join times as b on a.lastupdatetime = b.lastupdatetime
  )
  select
    partycode,
    count(distinct(coname)) as Total_scrips,
    sum(investment_value) investment_value,
    sum(protfolio_value) protfolio_value
  from
    final
  group by
    1
),
SGB as (
  select
    *
  from
    dbo_wms.SGB_Details
),
firstlogin as (
  select
    *
  from
    "dbo-angels"."product_analytics"."firstlogin"
),
MF as (
  SELECT
    sclientcode as client_id,
    lastorder,
    firstorder,
    current_value as portfolio,
    totalmforders
  FROM
    "dbo-angels"."dbo_wms"."tbl_mf_bee_spark_ordersummary"
),
device as (
  select
    client_id,
    max(
      case
        when device = 'SPARK Android' then network_type
      end
    ) "SPARK_Android_network_type",
    max(
      case
        when device = 'ABMA iOS' then network_type
      end
    ) "ABMA_iOS_network_type",
    max(
      case
        when device = 'ABMA Android' then network_type
      end
    ) "ABMA_ANDROID_network_type",
    max(
      case
        when device = 'SPARK iOS' then network_type
      end
    ) "SPARK_IOS_network_type",
    max(
      case
        when device = 'Spark Web' then device_model
      end
    ) "SPARK_WEB_device_model",
    max(
      case
        when device = 'TAB' then device_model
      end
    ) "TAB_device_model",
    max(
      case
        when device = 'SPARK Android' then device_model
      end
    ) "SPARK_Android_device_model",
    max(
      case
        when device = 'ABMA Android' then device_model
      end
    ) "ABMA_ANDROID_device_model",
    max(
      case
        when device = 'SPARK iOS' then device_model
      end
    ) "SPARK_IOS_device_model",
    max(
      case
        when device = 'ABMA iOS' then device_model
      end
    ) "ABMA_iOS_device_model"
  from
    product_analytics.clickstream_network_details
  group by
    client_id
),
FYR as (
  select
    party_code as client_id,
    FYR as first_year_revenue
  from
    (
      SELECT
        party_code,
        round(fyr, 0) FYR,
        row_number() over(
          partition by party_code
          order by
            dt desc
        ) rid
      FROM
        "dbo-angels"."online_engine_restricted"."sn_clientwisefyr"
    )
  where
    rid = 1
),
TB_lead as (
  select
    *
  from
    (
      SELECT
        party_code,
        ProspectID,
        mx_Application_Number,
        FirstName,
        CreatedOn,
        mx_Duplicati_Date,
        mx_Client_Code,
        mx_Offer_Type,
        mx_Offer,
        source,
        mx_Lead_Medium,
        mx_lead_type,
        Online_Source,
        Online_SubSource,
        Lead_Intent,
        HighPriority,
        Referal_Type,
        mx_DRACode,
        Final_Category,
        Lead_Type,
        New_FirstTimePage,
        mx_Application_Source,
        Web_App,
        OwnerIdEmailAddress,
        Final_Introducer,
        Emp_Name,
        TSM_Code,
        TSM_Name,
        DY_CH_Code,
        DY_CH_Name,
        CH_Code,
        CH_Name,
        ZH_Code,
        ZH_NAme,
        Functional_Role,
        Team_Category,
        Location,
        Region,
        Zone,
        mx_city,
        mx_State,
        State,
        Tier,
        mx_ReferringClientId,
        mx_Associate_Application_Number,
        mx_Sales_Lead_Status_First_Attempt,
        mx_Sales_Lead_Status_Last_Attempt,
        ProspectStage,
        Leads_received,
        Leads_attempted,
        Leads_Connected,
        Product_Pitched,
        Not_Intrested,
        Follow_up,
        MonthName,
        Year,
        mx_KYC_Journey,
        mx_Objection_Remark,
        Company,
        mx_Lead_Campaign,
        mx_TAT_For_Coding_Success,
        mx_Entry_URL,
        ReferralSourcewise,
        RM_Name,
        RM_Code,
        Under_Objection,
        Lead_Assignment_Date,
        Month,
        AUC,
        Last_Call_Date,
        Language_Barrier_status,
        Language_Barrier_Date,
        mx_TAT_For_Lead_Assignment,
        Lead_Attempt_Date,
        Call_Type,
        row_number() over(
          partition by party_code
          order by
            modifiedon desc
        ) rid
      FROM
        "dbo-angels"."dbo_sales_bi"."tb_lead"
    )
  where
    rid = 1
    and party_code is not null
),
AppVersion as (
  with rs as (
    SELECT
      client_id,
      max(dt),
      max(app_version_id) app_version_id,
      platform
    FROM
      "dbo-angels"."product_analytics"."overall_clients_appversion" a
      inner join "dbo-angels"."db_online_engine_rev"."sn_clientkyc" b on a.client_id = b.party_code
    where
      platform not in ('spark_web', 'tab')
      and dt >= current_date -456
    group by
      1,
      4
    order by
      client_id desc
  )
  select
    client_id,
    max(
      case
        when platform = 'abma_android' then app_version_id
      end
    ) as abma_android_app_version,
    max(
      case
        when platform = 'spark_android' then app_version_id
      end
    ) as spark_android_app_version,
    max(
      case
        when platform = 'spark_ios' then app_version_id
      end
    ) as spark_ios_app_version,
    max(
      case
        when platform = 'abma_ios' then app_version_id
      end
    ) as abma_ios_app_version
  from
    rs
  group by
    1
),
lastfundwithdraw as(
  SELECT
    *
  FROM
    dbo_funds.funds_max_withdraw
),
lastActivity as (
  SELECT
    *
  FROM
    "dbo-angels"."product_analytics"."last_activity"
),
lastTrade as (
    SELECT
        party_code,
        cast(first_delivery as date)first_delivery,
cast(first_intraday as date)first_intraday,
cast(first_commodity as date)first_commodity,
cast(first_currency as date)first_currency,
cast(first_fno as date)first_fno
    FROM
        "dbo-angels"."dbo_order"."first_trade"
),
firstTrade as (
    SELECT
        party_code,
cast(last_delivery as date)last_delivery,
cast(last_intraday as date)last_intraday,
cast(last_commodity as date)last_commodity,
cast(last_currency as date)last_currency,
cast(last_fno as date)last_fno
    FROM
        "dbo-angels"."dbo_order"."last_trade"
),
CTA as (
  select
    *
  from
    product_analytics.clevertap_cta
),
basic_info as (
  select
    party_code,
    lower(email) email,
    mobile_pager,
    lower(short_name) short_name
  from
    (
      SELECT
        email,
        mobile_pager,
        party_code,
        short_name,
        row_number() over(
          partition by party_code
          order by
            updation_date desc
        ) rid
      FROM
        "dbo-angels"."dbo_kycfulfillment"."client_details_kycmiddleware"
    )
  where
    rid = 1
),
sb_master as (
  select
    SBCode,
    Final_Tag,
    SUB_TAG_TYPE,
    PARENT_TAG,
    Terminal_YesNo_CTCL,
    Business_Model,
    SB_Name,
    SB_Mobile_No,
    SB_Emailid,
    NRM_Category_Last_Quarter,
    Mapping_Category
  from
    "sb_master_tables"."sb_master_final"
),
SFDC as (
  with rs as (
    select
      distinct id,
      client_id__c,
      status,
      origin,
      isclosed,
      closeddate,
      isclosedoncreate,
      createddate,
      case
        when isclosed = 'true' then DATEDIFF(day, createddate, closeddate)
        else null
      end AS TAT,
      is_reopen__c,
      survey_response__c,
      surveys_taken__c,
      case
        when level_1__c in (
          'Account Related (KYC, Profile, Segment)',
          'Account Related',
          'Account'
        ) then 'Account_related'
        when level_1__c in (
          'Funds, Limits and Margin',
          'MTF (Margin Trade Funding)'
        ) then 'Funds_related'
        when level_1__c in ('Internal Offers and Rewards', 'Internal Offers') then 'Offers_related'
        when level_1__c in ('Technical Issue') then 'Technical_Issue'
        when level_1__c in (
          'Order Placement',
          'Order Status',
          'Order Rejection',
          'Square-off'
        ) then 'Order_related'
        when level_1__c in (
          'Portfolio, Positions, and Corporate Actions',
          'Portfolio'
        ) then 'Portfolio_related'
        when level_1__c in ('Charges and Brokerage', 'Brokerage') then 'Charges_&_Brokerage'
        when level_1__c in ('IPO and OFS') then 'IPO_&_OFS'
        when level_1__c in ('Advisory and Alternate Products') then 'Advisor_&_Alternate_Products'
        when level_1__c in ('Statements') then 'Statements'
        when level_1__c in ('MF SIP', 'Mutual Funds', 'MF Redemption / Payout')
        or level_1__c like '%MF %'
        or level_1__c like '%Mutual %'
        or lower(level_1__c) like '%mutual fund%' then 'MF_related'
      end as Type
    from
      "dbo-angels"."product_analytics"."prod_sfdc_case"
    where
      length(client_id__c) > 1
  ),
  login as (
    select
      distinct client_id,
      last_login
    from
      LoginUsers
  )
  select
    client_id__c as client_id,
    count(id) as total_cases,
    sum(
      case
        when createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) total_cases_1year,
    sum(
      case
        when createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) total_cases_90Days,
    sum(
      case
        when createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) total_cases_60Days,
    sum(
      case
        when createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) total_cases_30Days,
    sum(
      case
        when createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) total_cases_7Days,
    sum(
      case
        when origin = 'Phone' then 1
        else 0
      end
    ) as Phone_origin_cases,
    sum(
      case
        when origin = 'Phone'
        and createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Phone_cases_1year,
    sum(
      case
        when origin = 'Phone'
        and createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Phone_cases_90Days,
    sum(
      case
        when origin = 'Phone'
        and createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Phone_cases_60Days,
    sum(
      case
        when origin = 'Phone'
        and createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Phone_cases_30Days,
    sum(
      case
        when origin = 'Phone'
        and createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Phone_cases_7Days,
    sum(
      case
        when origin = 'Angel assist' then 1
        else 0
      end
    ) as Angelassist_origin_cases,
    sum(
      case
        when origin = 'Angel assist'
        and createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Angelassist_cases_1year,
    sum(
      case
        when origin = 'Angel assist'
        and createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Angelassist_cases_90Days,
    sum(
      case
        when origin = 'Angel assist'
        and createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Angelassist_cases_60Days,
    sum(
      case
        when origin = 'Angel assist'
        and createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Angelassist_cases_30Days,
    sum(
      case
        when origin = 'Angel assist'
        and createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Angelassist_cases_7Days,
    sum(
      case
        when origin = 'Email' then 1
        else 0
      end
    ) as Email_origin_cases,
    sum(
      case
        when origin = 'Email'
        and createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Email_cases_1year,
    sum(
      case
        when origin = 'Email'
        and createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Email_cases_90Days,
    sum(
      case
        when origin = 'Email'
        and createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Email_cases_60Days,
    sum(
      case
        when origin = 'Email'
        and createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Email_cases_30Days,
    sum(
      case
        when origin = 'Email'
        and createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Email_cases_7Days,
    sum(
      case
        when isclosed = 'true' then 1
        else 0
      end
    ) as Total_Closed_cases,
    sum(
      case
        when isclosed = 'true'
        and createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as Closed_cases_1year,
    sum(
      case
        when isclosed = 'true'
        and createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as Closed_cases_90Days,
    sum(
      case
        when isclosed = 'true'
        and createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as Closed_cases_60Days,
    sum(
      case
        when isclosed = 'true'
        and createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as Closed_cases_30Days,
    sum(
      case
        when isclosed = 'true'
        and createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as Closed_cases_7Days,
    sum(
      case
        when isclosedoncreate = 'true' then 1
        else 0
      end
    ) as cases_closed_sameDay,
    sum(
      case
        when isclosedoncreate = 'true'
        and createddate >= last_login -365
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as closed_sameDay_1year,
    sum(
      case
        when isclosedoncreate = 'true'
        and createddate >= last_login -90
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as closed_sameDay_90Days,
    sum(
      case
        when isclosedoncreate = 'true'
        and createddate >= last_login -60
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as closed_sameDay_60Days,
    sum(
      case
        when isclosedoncreate = 'true'
        and createddate >= last_login -30
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as closed_sameDay_30Days,
    sum(
      case
        when isclosedoncreate = 'true'
        and createddate >= last_login -7
        and createddate <= last_login -1 then 1
        else 0
      end
    ) as closed_sameDay_7Days,
    sum(
      case
        when is_reopen__c = 'true' then 1
        else 0
      end
    ) as Re_open_cases,
    sum(
      case
        when surveys_taken__c = 'true' then 1
        else 0
      end
    ) as Total_surverys_responded,
    avg(
      case
        when length(survey_response__c) = 0 then null
        else survey_response__c
      end
    ) as Average_survey_rating,
    avg(
      case
        when createddate >= last_login -90
        and createddate <= last_login -1 then TAT
        else null
      end
    ) as Average_TAT_90days,
    avg(
      case
        when createddate >= last_login -60
        and createddate <= last_login -1 then TAT
        else null
      end
    ) as Average_TAT_60days,
    avg(
      case
        when createddate >= last_login -30
        and createddate <= last_login -1 then TAT
        else null
      end
    ) as Average_TAT_30days,
    avg(
      case
        when createddate >= last_login -7
        and createddate <= last_login -1 then TAT
        else null
      end
    ) as Average_TAT_7days,
    sum(
      case
        when Type = 'Account_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Account_related_last_6months,
    sum(
      case
        when Type = 'Funds_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Funds_related_last_6months,
    sum(
      case
        when Type = 'Offers_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Offers_related_last_6months,
    sum(
      case
        when Type = 'Technical_Issue'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Technical_Issue_last_6months,
    sum(
      case
        when Type = 'Order_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Order_related_last_6months,
    sum(
      case
        when Type = 'Charges_&_Brokerage'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Charges_N_Brokerage_last_6months,
    sum(
      case
        when Type = 'IPO_&_OFS'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) IPO_N_OFS_last_6months,
    sum(
      case
        when Type = 'Advisor_&_Alternate_Products'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Advisor_N_Alternate_Products_last_6months,
    sum(
      case
        when Type = 'Statements'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Statements_last_6months,
    sum(
      case
        when Type = 'Portfolio_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) Portfolio_related_last_6months,
    sum(
      case
        when Type = 'MF_related'
        and createddate >= last_login -180
        and createddate <= last_login -1 then 1
        else 0
      end
    ) MF_related_last_6months
  from
    rs a
    inner join login b on b.client_id = a.client_id__c
  group by
    1
),

Net_rev as (Select distinct clientcode, netbrokrev_LTV from "dbo_order"."Net_broker_rev_LTV"),
    Brokerage_summary as (SELECT clientcode,
sum(cast(noofdaystraded as decimal))noofdaystraded,
sum(cast(availabletradedays as decimal))availabletradedays,
sum(cast(grossbrokrev as decimal))grossbrokrev,
sum(cast(netbrokrev as decimal))netbrokrev,
sum(cast(neteq as decimal))neteq,
sum(cast(netfo as decimal))netfo,
sum(cast(netfut as decimal))netfut,
sum(cast(netopt as decimal))netopt,
sum(cast(netcomm as decimal))netcomm,
sum(cast(netcurr as decimal))netcurr,
sum(cast(netcommcurr as decimal))netcommcurr,
sum(cast(netonlinebrok as decimal))netonlinebrok,
sum(cast(noofdaystradedcash as decimal))noofdaystradedcash,
sum(cast(noofdaystradedfno as decimal))noofdaystradedfno,
sum(cast(noofdaystradedcomm as decimal))noofdaystradedcomm,
sum(cast(noofdaystradedcurr as decimal))noofdaystradedcurr,
sum(cast(noofdaystradedcommcurr as decimal))noofdaystradedcommcurr,
sum(cast(noofdaystradedfut as decimal))noofdaystradedfut,
sum(cast(noofdaystradedopt as decimal))noofdaystradedopt,
sum(cast(noofdaystradedonline as decimal))noofdaystradedonline,
sum(cast(noofdaystradedoffline as decimal))noofdaystradedoffline
 FROM "dbo-angels"."sb_master_tables"."dbo_ispc_master_scard_partyinfobrokmonthwise" 
 group by 1),

 first_activity as (SELECT *,
case   when first_activity_abma_android >= first_activity_abma_ios
                    and first_activity_abma_android >= first_activity_spark_ios
                    and first_activity_abma_android >= first_activity_spark_android
                    and first_activity_abma_android >= first_activity_spark_web
                    and first_activity_abma_android >= first_activity_tab_web then first_activity_abma_android
                    when first_activity_abma_ios >= first_activity_abma_android
                    and first_activity_abma_ios >= first_activity_spark_ios
                    and first_activity_abma_ios >= first_activity_spark_android
                    and first_activity_abma_ios >= first_activity_spark_web
                    and first_activity_abma_ios >= first_activity_tab_web then first_activity_abma_ios
                    when first_activity_spark_ios >= first_activity_abma_android
                    and first_activity_spark_ios >= first_activity_abma_ios
                    and first_activity_spark_ios >= first_activity_spark_android
                    and first_activity_spark_ios >= first_activity_spark_web
                    and first_activity_spark_ios >= first_activity_tab_web then first_activity_spark_ios
                    when first_activity_spark_android >= first_activity_abma_android
                    and first_activity_spark_android >= first_activity_abma_ios
                    and first_activity_spark_android >= first_activity_spark_ios
                    and first_activity_spark_android >= first_activity_spark_web
                    and first_activity_spark_android >= first_activity_tab_web then first_activity_spark_android
                    when first_activity_spark_web >= first_activity_abma_android
                    and first_activity_spark_web >= first_activity_abma_ios
                    and first_activity_spark_web >= first_activity_spark_ios
                    and first_activity_spark_web >= first_activity_spark_android
                    and first_activity_spark_web >= first_activity_tab_web then first_activity_spark_web
                    when first_activity_tab_web >= first_activity_abma_android
                    and first_activity_tab_web >= first_activity_abma_ios
                    and first_activity_tab_web >= first_activity_spark_ios
                    and first_activity_tab_web >= first_activity_spark_android
                    and first_activity_tab_web >= first_activity_spark_web then first_activity_tab_web end as first_activity 
from
                    (
                        Select
                            client_id,
                            case
                            when first_activity_abma_android is null then '1990-01-01'
                            else first_activity_abma_android end as first_activity_abma_android,
                            case
                            when first_activity_abma_ios is null then '1990-01-01'
                            else first_activity_abma_ios end as first_activity_abma_ios,
                            case
                            when first_activity_spark_android is null then '1990-01-01'
                            else first_activity_spark_android end as first_activity_spark_android,
                            case
                            when first_activity_spark_ios is null then '1990-01-01'
                            else first_activity_spark_ios end as first_activity_spark_ios,
                            case
                            when first_activity_spark_web is null then '1990-01-01'
                            else first_activity_spark_web end as first_activity_spark_web,
                            case
                            when first_activity_tab_web is null then '1990-01-01'
                            else first_activity_tab_web end as first_activity_tab_web
                        

 FROM "dbo-angels"."product_analytics"."first_activity" )),

add_funds as (
  select
    client_code,
    sum(
      case
        when date_diff('day', cast((transreq_dtm) as date), last_login) <= 30 then (amount)
        else 0
      end
    ) AddFund_Last30_days,
    sum(
      case
        when date_diff('day', cast((transreq_dtm) as date), last_login) <= 60 then (amount)
        else 0
      end
    ) AddFund_Last60_days,
    sum(
      case
        when date_diff('day', cast((transreq_dtm) as date), last_login) <= 90 then (amount)
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
        and cast(date(cast(transreq_dtm as timestamp)) as varchar) >= date_add('day', -456, current_date)
      group by
        client_code,
        transreq_dtm
    ) as a
    inner join LoginUsers b on b.client_id = a.client_code
  group by
    client_code
)
select
  distinct a.client_id,
  case
    when b2c_dra_tag = 'other' then B2C_tag
    when b2c_dra_tag is null then B2C_tag
    else b2c_dra_tag
  end as ClientType,
  Application_no,
  cast(partycode_gendate as date) partycode_gendate,
  cast(activefrom as date) activefrom,
  case
    when email_id is null then email
    else email_id
  end as email_id,
  case
    when mobile is null then mobile_pager
    else mobile
  end as mobile,
  case
    when clientname is null then short_name
    else clientname
  end as clientname,
  cast(birthdate as date) as birthdate,
  c.age,
  case
    when c.age < 18 then 'Less than 18'
    when c.age >= 18
    and c.age <= 25 then '18-25'
    when c.age >= 26
    and c.age <= 30 then '26-30'
    when c.age >= 31
    and c.age <= 35 then '31-35'
    when c.age >= 36
    and c.age <= 40 then '36-40'
    when c.age >= 41
    and c.age <= 45 then '41-45'
    when c.age >= 46
    and c.age <= 50 then '46-50'
    when c.age >= 51 then '50+'
  end as age_bucket,
  city_kyc,
  city_appsFlyer,
  Tier,
  state_kyc,
  state_appsFlyer,
  country_kyc,
  country_appsFlyer,
  operator,
  wifi,
  install_time_appsFlyer,
  Uninstall_time_appsFlyer,
  SBCode,
  Final_Tag,
  SUB_TAG_TYPE,
  PARENT_TAG,
  Terminal_YesNo_CTCL,
  Business_Model,
  SB_Name,
  SB_Mobile_No,
  SB_Emailid,
  NRM_Category_Last_Quarter,
  Mapping_Category,
  L1_tags,
  L2_tags,
  kyc_type,
  diy,
  diy_type,
  scheme_name,
  AUC,
  cast(CreatedOn as date) CreatedOn,
  MonthName,
  Year,
  cast(mx_Duplicati_Date as date) mx_Duplicati_Date,
  mx_Offer_Type,
  mx_Offer,
  source,
  mx_Lead_Medium,
  mx_lead_type,
  Online_Source,
  Online_SubSource,
  Lead_Intent,
  HighPriority,
  Referal_Type,
  mx_DRACode,
  Final_Category,
  Lead_Type,
  New_FirstTimePage as First_time_page_drop,
  mx_Application_Source,
  Web_App,
  OwnerIdEmailAddress,
  cast(Lead_Assignment_Date as date) Lead_Assignment_Date,
  mx_TAT_For_Lead_Assignment as TAT_Calling_Assignment,
  cast(Lead_Attempt_Date as date) as First_lead_attempt_date,
  Last_Call_Date,
  Company as Sub_disposition,
  Final_Introducer as DAE_Ecode,
  Emp_Name as DAE_Name,
  TSM_Code,
  TSM_Name,
  DY_CH_Code,
  DY_CH_Name,
  CH_Code,
  CH_Name,
  ZH_Code,
  ZH_NAme,
  Functional_Role,
  Team_Category,
  Location,
  Region,
  Zone,
  mx_ReferringClientId as mx_Referring_client_ID,
  mx_Associate_Application_Number as final_referring_client_ID,
  mx_Sales_Lead_Status_First_Attempt,
  mx_Sales_Lead_Status_Last_Attempt,
  ProspectStage as Disposition,
  Leads_received,
  Leads_attempted,
  Leads_Connected,
  Product_Pitched,
  Not_Intrested,
  Follow_up,
  mx_KYC_Journey,
  mx_Objection_Remark,
  mx_Lead_Campaign,
  mx_TAT_For_Coding_Success,
  mx_Entry_URL,
  ReferralSourcewise,
  RM_Name,
  RM_Code,
  Under_Objection,
  Month,
  Language_Barrier_status,
  Language_Barrier_Date,
  Total_scrips as EQ_Portfolio_Total_Scrips,
  investment_value as EQ_Portfolio_Investment_Value,
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
  cast(Date_Active_BSE as date) as Date_Active_BSE,
  cast(Date_Inactive_BSE as date) as Date_Inactive_BSE,
  cast(Date_Active_NSE as date) as Date_Active_NSE,
  cast(Date_Inactive_NSE as date) as Date_Inactive_NSE,
  cast(DateFirstFundAdded as date) DateFirstFundAdded,
  cast(DateLastFundAdded as date) as DateLastFundAdded,
  cast(last_withdrawn_date_abma_android as date) last_withdrawn_date_abma_android,
  cast(last_withdrawn_date_spark_android as date) last_withdrawn_date_spark_android,
  cast(last_withdrawn_date_abma_ios as date) last_withdrawn_date_abma_ios,
  cast(last_withdrawn_date_spark_ios as date) last_withdrawn_date_spark_ios,
  nvl(
    cast(last_withdrawn_amount_spark_android as float),
    0
  ) as last_withdrawn_amount_spark_android,
  nvl(
    cast(last_withdrawn_amount_spark_ios as float),
    0
  ) as last_withdrawn_amount_spark_ios,
  AddFund_Last30_days,
  AddFund_Last60_days,
  AddFund_Last90_days,
  cast(DateFirstTrade as date) DateFirstTrade,
  cast(DateLastTrade as date) DateLastTrade,
 Total_orders_placed,
total_gross_brokerage_ltv,
netbrokrev_LTV,
noofdaystraded,
availabletradedays,
grossbrokrev,
netbrokrev,
neteq,
netfo,
netfut,
netopt,
netcomm,
netcurr,
netcommcurr,
netonlinebrok,
noofdaystradedcash,
noofdaystradedfno,
noofdaystradedcomm,
noofdaystradedcurr,
noofdaystradedcommcurr,
noofdaystradedfut,
noofdaystradedopt,
noofdaystradedonline,
noofdaystradedoffline,
  Daily_avg_last90_days,
  Daily_avg_last60_days,
  Daily_avg_last30_days,
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
  first_year_revenue,

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
 cast(first_delivery as date)first_delivery,
cast(last_delivery as date)last_delivery,
cast(first_intraday as date)first_intraday,
cast(last_intraday as date)last_intraday,
cast(first_commodity as date)first_commodity,
cast(last_commodity as date)last_commodity,
cast(first_currency as date)first_currency,
cast(last_currency as date)last_currency,
cast(first_fno as date)first_fno,
cast(last_fno as date)last_fno,
  totalmforders as TotalMFOrderCount,
  round(portfolio) as MFPortfolioValue,
  cast(firstorder as date) as DateFirstTradeMF,
  cast(lastorder as date) as DateLastTradeMF,
  cast(total_bond_orders as int) as TotalSGBOrderCount,
  cast(first_bond_order as date) as DateFirstTradeSGB,
  cast(last_bond_order as date) as DateLastTradeSGB,
  TotaluniqueIPOapplied1yr,
  TotalIPOapplied1yr,
  cast(h.DateFirstIPOApplied as date) as DateFirstIPOApplied,
  cast(h.DateLastIPOApplied as date) as DateLastIPOApplied,
  -- 0 as TotalGTTordercount,
  -- cast('01-01-9999' as date) as DateFirstGTTOrder,
  -- cast('01-01-9999' as date) as DateLastGTTOrder,
  -- 0 as StockSIPOrdercount,
  -- cast('01-01-9999' as date) as DateFirstSIPOrder,
  -- cast('01-01-9999' as date) as DateLastSIPOrder,
  total_nps_response,
  detractor,
  promoter,
  total_nps_response_portfolio,
  total_nps_response_ipo,
  total_nps_response_charts,
  total_nps_response_addfunds,
  total_nps_response_orders,
  total_nps_response_kyc,
  Total_nps_response_watchlist,
  Total_nps_response_homepage,
  Total_nps_response_reports,
  Total_nps_response_insta_trade,
--   Total_nps_response_insta_trade,
  Total_nps_response_first_buy_order,
  Total_nps_response_KYC2,
  Total_nps_response_MTF,
  total_cta_last90days,
  android_inapp_cta_last90days,
  android_email_cta_last90days,
  android_push_cta_last90days,
  android_browser_cta_last90days,
  ios_inapp_cta_last90days,
  ios_email_cta_last90days,
  ios_push_cta_last90days,
  ios_browser_cta_last90days,
  web_inapp_cta_last90days,
  web_email_cta_last90days,
  web_push_cta_last90days,
  web_browser_cta_last90days,
  cast(last_cta_timestamp as date) as last_cta_timestamp,
  spark_android_network_type,
  abma_android_network_type,
  spark_ios_network_type,
  abma_ios_network_type,
  spark_android_device_model,
  abma_android_device_model,
  spark_ios_device_model,
  spark_web_device_model,
  tab_device_model,
  abma_ios_device_model,
  Device_make_appsFlyer,
  Device_Model_appsFlyer,
  app_versionappsFlyer,
  abma_android_app_version,
  spark_android_app_version,
  spark_ios_app_version,
  abma_ios_app_version,
-- case when first_login>first_activity then first_activity else first_login end as first_login,
    -- case when abma_android_first_login>first_activity_abma_android then first_activity_abma_android else abma_android_first_login end as abma_android_first_login,
    -- case when abma_ios_first_login> first_activity_abma_ios then first_activity_abma_ios  else abma_ios_first_login end as abma_ios_first_login ,
    -- case when spark_android_first_login> first_activity_spark_android then first_activity_spark_android else spark_android_first_login end as spark_android_first_login ,
    -- case when spark_ios_first_login> first_activity_spark_ios then first_activity_spark_ios else spark_ios_first_login end as spark_ios_first_login,
    -- case when tab_first_login> first_activity_tab_web then first_activity_tab_web else tab_first_login end as tab_first_login,
    -- case when spark_web_first_login> first_activity_spark_web then first_activity_spark_web else spark_web_first_login end as spark_web_first_login,

  first_login,
   abma_android_first_login,
  abma_ios_first_login,
  spark_android_first_login,
  spark_ios_first_login,
   tab_first_login,
  spark_web_first_login,
  Lastlogin_appsFlyer,
  last_login,
  cast(ABMAAndroid_last_login as date) ABMAAndroid_last_login,
  cast(ABMAIOS_last_login as date) ABMAIOS_last_login,
  cast(SPARKAndroid_last_login as date) SPARKAndroid_last_login,
  cast(SPARKIOS_last_login as date) SPARKIOS_last_login,
  cast(TAB_last_login as date) TAB_last_login,
  cast(Sparkweb_last_login as date) Sparkweb_last_login,
  total_cases,
  total_cases_1year,
  total_cases_90days,
  total_cases_60days,
  total_cases_30days,
  total_cases_7days,
  phone_origin_cases,
  phone_cases_1year,
  phone_cases_90days,
  phone_cases_60days,
  phone_cases_30days,
  phone_cases_7days,
  angelassist_origin_cases,
  angelassist_cases_1year,
  angelassist_cases_90days,
  angelassist_cases_60days,
  angelassist_cases_30days,
  angelassist_cases_7days,
  email_origin_cases,
  email_cases_1year,
  email_cases_90days,
  email_cases_60days,
  email_cases_30days,
  email_cases_7days,
  total_closed_cases,
  closed_cases_1year,
  closed_cases_90days,
  closed_cases_60days,
  closed_cases_30days,
  closed_cases_7days,
  cases_closed_sameday,
  closed_sameday_1year,
  closed_sameday_90days,
  closed_sameday_60days,
  closed_sameday_30days,
  closed_sameday_7days,
  re_open_cases,
  total_surverys_responded,
  average_survey_rating,
  Average_TAT_90days,
  Average_TAT_60days,
  Average_TAT_30days,
  Average_TAT_7days,
  Account_related_last_6months,
  funds_related_last_6months,
  offers_related_last_6months,
  technical_issue_last_6months,
  order_related_last_6months,
  charges_n_brokerage_last_6months,
  ipo_n_ofs_last_6months,
  advisor_n_alternate_products_last_6months,
  statements_last_6months,
  portfolio_related_last_6months,
  mf_related_last_6months,
  current_date as dt
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
  left join portfolio t on a.client_id = t.partycode
  left join SGB u on a.client_id = u.client_id
  left join firstlogin v on a.client_id = v.client_id
  left join MF w on a.client_id = w.client_id
  left join device x on a.client_id = x.client_id
  left join FYR y on a.client_id = y.client_id
  left join CTA z on a.client_id = z.client_id
  left join TB_lead a1 on a.client_id = a1.party_code
  left join AppVersion b1 on a.client_id = b1.client_id
  left join AppsFlyer s on a.Client_id = s.Client_id
  left join lastfundwithdraw a2 on a2.client_id = a.client_id
  left join lastTrade a3 on a3.party_code = a.client_id
  left join firstTrade a4 on a4.party_code = a.client_id
  left join basic_info a5 on a5.party_code = a.client_id
  left join sb_master a6 on a6.sbcode = c.sub_broker
  left join Sparkweb a7 on a.client_id = a7.client_id
  left join SFDC a8 on a.client_id = a8.client_id
-- left join LoginUsersv2 a9 on a.client_id = a9.client_id
Left join Net_rev a10 on a.client_id = a10.clientcode
left join Brokerage_summary a11 on a.client_id = a11.clientcode
    left join first_activity a12 on a.client_id = a12.client_id;

b