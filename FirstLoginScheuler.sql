create temp table firstlogin_temp(
  like "dbo-angels"."product_analytics"."firstlogin"
);
insert into
  firstlogin_temp with rs as (
    select
      client_id,
      dt as dt,
      'ABMA IOS' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_abma_ios_clickstream_data"
    where
      event_name = 'LoginValidation'
      and (event_metadata like '%Status":"success%')
      and dt = current_date -1
    union all
    select
      client_id,
      dt as dt,
      'ABMA Android' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_dp_abma_clickstream_data"
    where
      event_name = 'LoginValidation'
      and (
        event_metadata like '%"Successful":"Y"%'
        or event_metadata like '% Status: "Success"%'
        or event_metadata LIKE '%"Status":"Success"%'
      )
      and dt = current_date -1
    union all
    select
      client_id,
      dt as dt,
      'Spark Android' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_dp_spark_clickstream_data"
    where
      event_id = '1.0.0.0.1'
      and event_metadata like '%status: success%'
      and dt = current_date -1
    union all
    select
      client_id,
      dt as dt,
      'Spark IOS' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_spark_ios_clickstream_data"
    where
      event_id = '1.0.0.0.1'
      and event_metadata like '%status":"success%'
      and dt = current_date -1
    union all
    select
      client_id,
      dt as dt,
      'TAB' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_tab_clickstream_data"
    where
      event_id = '12.6.1.0.0'
      and (event_metadata like '%Status: Success%')
      and dt = current_date -1
    union all
    select
      client_id,
      dt as dt,
      'Spark Web' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_rtab_clickstream_data"
    where
      event_id = '1.0.0.9.4'
      and dt = current_date -1
  ),
  kyc as(
    select
      party_code,
      split_part(activefrom, ' ', 1) as active_dt
    from
      db_online_engine_rev.sn_clientkyc
    where
      activefrom >= '2021-04-01'
  )
select
  client_id,
  First_login,
  ABMA_Android_First_Login,
  ABMA_IOS_First_Login,
  Spark_Android_First_Login,
  Spark_IOS_First_Login,
  TAB_First_Login,
  Spark_Web_First_Login,
  onboarding_date
from
  (
    select
      a.client_id client_id,
      min(cast(a.dt as date)) over (partition by a.client_id) as First_login,
      case
        when Device = 'ABMA Android' then cast(a.dt as date)
      end as ABMA_Android_First_Login,
      case
        when Device = 'ABMA IOS' then cast(a.dt as date)
      end as ABMA_IOS_First_Login,
      case
        when Device = 'Spark Android' then cast(a.dt as date)
      end as Spark_Android_First_Login,
      case
        when Device = 'Spark_IOS' then cast(a.dt as date)
      end as Spark_IOS_First_Login,
      case
        when Device = 'TAB' then cast(a.dt as date)
      end as TAB_First_Login,
      case
        when Device = 'Spark Web' then cast(a.dt as date)
      end as Spark_Web_First_Login,
      cast(b.active_dt as date) as onboarding_date,
      row_number() over(
        partition by a.client_id
        order by
          a.dt desc
      ) rid
    from
      rs a
      left join kyc b on a.client_id = b.party_code
  )
where
  rid = 1;begin TRANSACTION;
update
  "dbo-angels"."product_analytics"."firstlogin" a
set
  first_login = nvl(a.first_login, b.first_login),
  abma_android_first_login = nvl(
    a.abma_android_first_login,
    b.abma_android_first_login
  ),
  abma_ios_first_login = nvl(a.abma_ios_first_login, b.abma_ios_first_login),
  spark_android_first_login = nvl(
    a.spark_android_first_login,
    b.spark_android_first_login
  ),
  spark_ios_first_login = nvl(a.spark_ios_first_login, b.spark_ios_first_login),
  tab_first_login = nvl(a.tab_first_login, b.tab_first_login),
  spark_web_first_login = nvl(a.spark_web_first_login, b.spark_web_first_login),
  onboarding_date = nvl(a.onboarding_date, b.onboarding_date)
from
  firstlogin_temp b
where
  a.client_id = b.client_id
  and (
    a.first_login is null
    or a.abma_android_first_login is null
    or a.abma_ios_first_login is null
    or a.spark_android_first_login is null
    or a.spark_ios_first_login is null
    or a.tab_first_login is null
    or a.spark_web_first_login is null
    or a.onboarding_date is null
  );
insert into "dbo-angels"."product_analytics"."firstlogin"
select * from firstlogin_temp where client_id in  (
    select
      client_id
    from
      firstlogin_temp minus
    select
      client_id
    from
      "dbo-angels"."product_analytics"."firstlogin"
  );
end TRANSACTION;
drop table firstlogin_temp;