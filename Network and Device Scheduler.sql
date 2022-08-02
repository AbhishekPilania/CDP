create temp table ck_netwrok_stg (like product_analytics.clickstream_network_details);

insert into ck_netwrok_stg
 select * from (
select
  client_id,
  case
    when event_metadata like '%cellularnetwork: 4G%' then '4G'
    when event_metadata like '%cellularnetwork: WIFI%' then 'WIFI'
    when event_metadata like '%cellularnetwork: -%' then 'Undefined'
    when event_metadata like '%cellularnetwork: 3G%' then '3G'
    when event_metadata like '%cellularnetwork: NA%' then 'Undefined'
    when event_metadata like '%cellularnetwork: 2G%' then '2G'
    when event_metadata like '%cellularnetwork: ?%' then 'Undefined'
    else 'Undefined'
  end as Network_type,
  split_part(
    split_part(split_part(event_metadata, '{', 6), ':', 2),
    '}',
    1
  ) as Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    device_model
  ) as Device_model,
  'ABMA Android' as Device,
  cast(dt as date) dt
from
  "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_dp_abma_clickstream_data"
where
  event_id in('0.0.0.139.0.0', '0.0.0.142.0.0')
  and dt = current_date -1
  and length(client_id) > 1
  and length(event_metadata) > 1
union all
select
  client_id,
  case
    when event_metadata like '%cellularnetwork: 4G%' then '4G'
    when event_metadata like '%cellularnetwork: WIFI%' then 'WIFI'
    when event_metadata like '%cellularnetwork: -%' then 'Undefined'
    when event_metadata like '%cellularnetwork: 3G%' then '3G'
    when event_metadata like '%cellularnetwork: NA%' then 'Undefined'
    when event_metadata like '%cellularnetwork: 2G%' then '2G'
    when event_metadata like '%cellularnetwork: ?%' then 'Undefined'
    else 'Undefined'
  end as Network_type,
  split_part(
    split_part(split_part(event_metadata, '{', 6), ':', 2),
    '}',
    1
  ) as Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    device_model
  ) as Device_model,
  'SPARK Android' as Device,
    cast(dt as date) dt
from
  "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_dp_spark_clickstream_data"
where
  event_id in('0.0.0.0.1', '0.0.0.0.4')
  and dt = current_date -1
  and length(client_id) > 1
  and length(event_metadata) > 1
union all
select
  client_id,
  Network_type,
  Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    Model
  ) as Device_model,
  Device,
    cast(dt as date) dt
from
  (
    select
      dt,
      client_id,
      case
        when event_metadata like '%cellularnetwork: 4G%' then '4G'
        when event_metadata like '%cellularnetwork: WIFI%' then 'WIFI'
        when event_metadata like '%cellularnetwork: -%' then 'Undefined'
        when event_metadata like '%cellularnetwork: 3G%' then '3G'
        when event_metadata like '%cellularnetwork: NA%' then 'Undefined'
        when event_metadata like '%cellularnetwork: 2G%' then '2G'
        when event_metadata like '%cellularnetwork: ?%' then 'Undefined'
        else 'Undefined'
      end as Network_type,
      split_part(
        split_part(split_part(event_metadata, '{', 6), ':', 2),
        '}',
        1
      ) as Service_Provider,
      device_manufacturer,
      case
        when device_model = 'iPhone1,1' then 'iPhone'
        when device_model = 'iPhone1,2' then 'iPhone 3G'
        when device_model = 'iPhone2,1' then 'iPhone 3GS'
        when device_model = 'iPhone3,1' then 'iPhone 4'
        when device_model = 'iPhone3,2' then 'iPhone 4 GSM Rev A'
        when device_model = 'iPhone3,3' then 'iPhone 4 CDMA'
        when device_model = 'iPhone4,1' then 'iPhone 4S'
        when device_model = 'iPhone5,1' then 'iPhone 5'
        when device_model = 'iPhone5,2' then 'iPhone 5'
        when device_model = 'iPhone5,3' then 'iPhone 5C'
        when device_model = 'iPhone5,4' then 'iPhone 5C'
        when device_model = 'iPhone6,1' then 'iPhone 5S'
        when device_model = 'iPhone6,2' then 'iPhone 5S'
        when device_model = 'iPhone7,1' then 'iPhone 6 Plus'
        when device_model = 'iPhone7,2' then 'iPhone 6'
        when device_model = 'iPhone8,1' then 'iPhone 6s'
        when device_model = 'iPhone8,2' then 'iPhone 6s Plus'
        when device_model = 'iPhone8,4' then 'iPhone SE'
        when device_model = 'iPhone9,1' then 'iPhone 7'
        when device_model = 'iPhone9,2' then 'iPhone 7 Plus'
        when device_model = 'iPhone9,3' then 'iPhone 7'
        when device_model = 'iPhone9,4' then 'iPhone 7 Plus'
        when device_model = 'iPhone10,1' then 'iPhone 8'
        when device_model = 'iPhone10,2' then 'iPhone 8 Plus'
        when device_model = 'iPhone10,3' then 'iPhone X Global'
        when device_model = 'iPhone10,4' then 'iPhone 8'
        when device_model = 'iPhone10,5' then 'iPhone 8 Plus'
        when device_model = 'iPhone10,6' then 'iPhone X GSM'
        when device_model = 'iPhone11,2' then 'iPhone XS'
        when device_model = 'iPhone11,4' then 'iPhone XS Max'
        when device_model = 'iPhone11,6' then 'iPhone XS Max Global'
        when device_model = 'iPhone11,8' then 'iPhone XR'
        when device_model = 'iPhone12,1' then 'iPhone 11'
        when device_model = 'iPhone12,3' then 'iPhone 11 Pro'
        when device_model = 'iPhone12,5' then 'iPhone 11 Pro Max'
        when device_model = 'iPhone12,8' then 'iPhone SE 2nd Gen'
        when device_model = 'iPhone13,1' then 'iPhone 12 Mini'
        when device_model = 'iPhone13,2' then 'iPhone 12'
        when device_model = 'iPhone13,3' then 'iPhone 12 Pro'
        when device_model = 'iPhone13,4' then 'iPhone 12 Pro Max'
        when device_model = 'iPhone14,2' then 'iPhone 13 Pro'
        when device_model = 'iPhone14,3' then 'iPhone 13 Pro Max'
        when device_model = 'iPhone14,4' then 'iPhone 13 Mini'
        when device_model = 'iPhone14,5' then 'iPhone 13'
        when device_model = 'iPhone14,6' then 'iPhone SE 3rd Gen'
        when device_model = 'iPad1,1' then 'iPad'
        when device_model = 'iPad1,2' then 'iPad 3G'
        when device_model = 'iPad2,1' then '2nd Gen iPad'
        when device_model = 'iPad2,2' then '2nd Gen iPad GSM'
        when device_model = 'iPad2,3' then '2nd Gen iPad CDMA'
        when device_model = 'iPad2,4' then '2nd Gen iPad New Revision'
        when device_model = 'iPad3,1' then '3rd Gen iPad'
        when device_model = 'iPad3,2' then '3rd Gen iPad CDMA'
        when device_model = 'iPad3,3' then '3rd Gen iPad GSM'
        when device_model = 'iPad2,5' then 'iPad mini'
        when device_model = 'iPad2,6' then 'iPad mini GSM+LTE'
        when device_model = 'iPad2,7' then 'iPad mini CDMA+LTE'
        when device_model = 'iPad3,4' then '4th Gen iPad'
        when device_model = 'iPad3,5' then '4th Gen iPad GSM+LTE'
        when device_model = 'iPad3,6' then '4th Gen iPad CDMA+LTE'
        when device_model = 'iPad4,1' then 'iPad Air'
        when device_model = 'iPad4,2' then 'iPad Air'
        when device_model = 'iPad4,3' then '1st Gen iPad Air'
        when device_model = 'iPad4,4' then 'iPad mini Retina'
        when device_model = 'iPad4,5' then 'iPad mini Retina'
        when device_model = 'iPad4,6' then 'iPad mini Retina'
        when device_model = 'iPad4,7' then 'iPad mini 3'
        when device_model = 'iPad4,8' then 'iPad mini 3'
        when device_model = 'iPad4,9' then 'iPad Mini 3'
        when device_model = 'iPad5,1' then 'iPad mini 4'
        when device_model = 'iPad5,2' then '4th Gen iPad mini'
        when device_model = 'iPad5,3' then 'iPad Air 2'
        when device_model = 'iPad5,4' then 'iPad Air 2'
        when device_model = 'iPad6,3' then 'iPad Pro'
        when device_model = 'iPad6,4' then 'iPad Pro'
        when device_model = 'iPad6,7' then 'iPad Pro'
        when device_model = 'iPad6,8' then 'iPad Pro'
        when device_model = 'iPad6,11' then 'iPad'
        when device_model = 'iPad6,12' then 'iPad'
        when device_model = 'iPad7,1' then 'iPad Pro 2nd Gen'
        when device_model = 'iPad7,2' then 'iPad Pro 2nd Gen'
        when device_model = 'iPad7,3' then 'iPad Pro 10'
        when device_model = 'iPad7,4' then 'iPad Pro 10'
        when device_model = 'iPad7,5' then 'iPad 6th Gen'
        when device_model = 'iPad7,6' then 'iPad 6th Gen'
        when device_model = 'iPad7,11' then 'iPad 7th Gen 10'
        when device_model = 'iPad7,12' then 'iPad 7th Gen 10'
        when device_model = 'iPad8,1' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,2' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,3' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,4' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,5' then 'iPad Pro 12'
        when device_model = 'iPad8,6' then 'iPad Pro 12'
        when device_model = 'iPad8,7' then 'iPad Pro 12'
        when device_model = 'iPad8,8' then 'iPad Pro 12'
        when device_model = 'iPad8,9' then 'iPad Pro 11 inch 4th Gen'
        when device_model = 'iPad8,10' then 'iPad Pro 11 inch 4th Gen'
        when device_model = 'iPad8,11' then 'iPad Pro 12'
        when device_model = 'iPad8,12' then 'iPad Pro 12'
        when device_model = 'iPad11,1' then 'iPad mini 5th Gen'
        when device_model = 'iPad11,2' then 'iPad mini 5th Gen'
        when device_model = 'iPad11,3' then 'iPad Air 3rd Gen'
        when device_model = 'iPad11,4' then 'iPad Air 3rd Gen'
        when device_model = 'iPad11,6' then 'iPad 8th Gen'
        when device_model = 'iPad11,7' then 'iPad 8th Gen'
        when device_model = 'iPad12,1' then 'iPad 9th Gen'
        when device_model = 'iPad12,2' then 'iPad 9th Gen'
        when device_model = 'iPad14,1' then 'iPad mini 6th Gen'
        when device_model = 'iPad14,2' then 'iPad mini 6th Gen'
        when device_model = 'iPad13,1' then 'iPad Air 4th Gen'
        when device_model = 'iPad13,2' then 'iPad Air 4th Gen'
        when device_model = 'iPad13,4' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,5' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,6' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,7' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,8' then 'iPad Pro 12'
        when device_model = 'iPad13,9' then 'iPad Pro 12'
        when device_model = 'iPad13,10' then 'iPad Pro 12'
        when device_model = 'iPad13,11' then 'iPad Pro 12'
        when device_model = 'iPad13,16' then 'iPad Air 5th Gen'
        when device_model = 'iPad13,17' then 'iPad Air 5th Gen'
        else device_model
      end as Model,
      'ABMA iOS' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_abma_ios_clickstream_data"
    where
      event_id in('0.0.0.139.0.0', '0.0.0.142.0.0')
      and dt = current_date -1
      and length(client_id) > 1
      and length(event_metadata) > 1
  )
union all
select
  client_id,
  Network_type,
  Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    Model
  ) as Device_model,
  Device,
    cast(dt as date) dt
from(
    select
      dt,
      client_id,
      event_metadata,
      case
        when event_metadata like '%"cellularnetwork":"4G"%'
        and event_metadata like '%"internetconnection":"Cellular"%' then '4G'
        when event_metadata like '%cellularnetwork: WIFI%'
        and event_metadata like '%"internetconnection":"Cellular"%' then 'WIFI'
        when event_metadata like '%cellularnetwork: -%'
        and event_metadata like '%"internetconnection":"Cellular"%' then 'Undefined'
        when event_metadata like '%cellularnetwork: 3G%'
        and event_metadata like '%"internetconnection":"Cellular"%' then '3G'
        when event_metadata like '%cellularnetwork: NA%'
        and event_metadata like '%"internetconnection":"Cellular"%' then 'Undefined'
        when event_metadata like '%"cellularnetwork":"2G"%'
        and event_metadata like '%"internetconnection":"Cellular"%' then '2G'
        when event_metadata like '%"cellularnetwork":"2G"%'
        and event_metadata not like '%"internetconnection":"Wifi"%' then '2G'
        when event_metadata like '%"cellularnetwork":"3G"%'
        and event_metadata not like '%"internetconnection":"Wifi"%' then '3G'
        when event_metadata like '%"cellularnetwork":"4G"%'
        and event_metadata not like '%"internetconnection":"Wifi"%' then '4G'
        when event_metadata like '%cellularnetwork: ?%'
        and event_metadata like '%"internetconnection":"Cellular"%' then 'Undefined'
        when event_metadata like '%"internetconnection":"Wifi"%' then 'WIFI'
        else 'Undefined'
      end as Network_type,
      split_part(
        split_part(split_part(event_metadata, '{', 6), ':', 2),
        '}',
        1
      ) as Service_Provider,
      device_manufacturer,
      case
        when device_model = 'iPhone1,1' then 'iPhone'
        when device_model = 'iPhone1,2' then 'iPhone 3G'
        when device_model = 'iPhone2,1' then 'iPhone 3GS'
        when device_model = 'iPhone3,1' then 'iPhone 4'
        when device_model = 'iPhone3,2' then 'iPhone 4 GSM Rev A'
        when device_model = 'iPhone3,3' then 'iPhone 4 CDMA'
        when device_model = 'iPhone4,1' then 'iPhone 4S'
        when device_model = 'iPhone5,1' then 'iPhone 5'
        when device_model = 'iPhone5,2' then 'iPhone 5'
        when device_model = 'iPhone5,3' then 'iPhone 5C'
        when device_model = 'iPhone5,4' then 'iPhone 5C'
        when device_model = 'iPhone6,1' then 'iPhone 5S'
        when device_model = 'iPhone6,2' then 'iPhone 5S'
        when device_model = 'iPhone7,1' then 'iPhone 6 Plus'
        when device_model = 'iPhone7,2' then 'iPhone 6'
        when device_model = 'iPhone8,1' then 'iPhone 6s'
        when device_model = 'iPhone8,2' then 'iPhone 6s Plus'
        when device_model = 'iPhone8,4' then 'iPhone SE'
        when device_model = 'iPhone9,1' then 'iPhone 7'
        when device_model = 'iPhone9,2' then 'iPhone 7 Plus'
        when device_model = 'iPhone9,3' then 'iPhone 7'
        when device_model = 'iPhone9,4' then 'iPhone 7 Plus'
        when device_model = 'iPhone10,1' then 'iPhone 8'
        when device_model = 'iPhone10,2' then 'iPhone 8 Plus'
        when device_model = 'iPhone10,3' then 'iPhone X Global'
        when device_model = 'iPhone10,4' then 'iPhone 8'
        when device_model = 'iPhone10,5' then 'iPhone 8 Plus'
        when device_model = 'iPhone10,6' then 'iPhone X GSM'
        when device_model = 'iPhone11,2' then 'iPhone XS'
        when device_model = 'iPhone11,4' then 'iPhone XS Max'
        when device_model = 'iPhone11,6' then 'iPhone XS Max Global'
        when device_model = 'iPhone11,8' then 'iPhone XR'
        when device_model = 'iPhone12,1' then 'iPhone 11'
        when device_model = 'iPhone12,3' then 'iPhone 11 Pro'
        when device_model = 'iPhone12,5' then 'iPhone 11 Pro Max'
        when device_model = 'iPhone12,8' then 'iPhone SE 2nd Gen'
        when device_model = 'iPhone13,1' then 'iPhone 12 Mini'
        when device_model = 'iPhone13,2' then 'iPhone 12'
        when device_model = 'iPhone13,3' then 'iPhone 12 Pro'
        when device_model = 'iPhone13,4' then 'iPhone 12 Pro Max'
        when device_model = 'iPhone14,2' then 'iPhone 13 Pro'
        when device_model = 'iPhone14,3' then 'iPhone 13 Pro Max'
        when device_model = 'iPhone14,4' then 'iPhone 13 Mini'
        when device_model = 'iPhone14,5' then 'iPhone 13'
        when device_model = 'iPhone14,6' then 'iPhone SE 3rd Gen'
        when device_model = 'iPad1,1' then 'iPad'
        when device_model = 'iPad1,2' then 'iPad 3G'
        when device_model = 'iPad2,1' then '2nd Gen iPad'
        when device_model = 'iPad2,2' then '2nd Gen iPad GSM'
        when device_model = 'iPad2,3' then '2nd Gen iPad CDMA'
        when device_model = 'iPad2,4' then '2nd Gen iPad New Revision'
        when device_model = 'iPad3,1' then '3rd Gen iPad'
        when device_model = 'iPad3,2' then '3rd Gen iPad CDMA'
        when device_model = 'iPad3,3' then '3rd Gen iPad GSM'
        when device_model = 'iPad2,5' then 'iPad mini'
        when device_model = 'iPad2,6' then 'iPad mini GSM+LTE'
        when device_model = 'iPad2,7' then 'iPad mini CDMA+LTE'
        when device_model = 'iPad3,4' then '4th Gen iPad'
        when device_model = 'iPad3,5' then '4th Gen iPad GSM+LTE'
        when device_model = 'iPad3,6' then '4th Gen iPad CDMA+LTE'
        when device_model = 'iPad4,1' then 'iPad Air'
        when device_model = 'iPad4,2' then 'iPad Air'
        when device_model = 'iPad4,3' then '1st Gen iPad Air'
        when device_model = 'iPad4,4' then 'iPad mini Retina'
        when device_model = 'iPad4,5' then 'iPad mini Retina'
        when device_model = 'iPad4,6' then 'iPad mini Retina'
        when device_model = 'iPad4,7' then 'iPad mini 3'
        when device_model = 'iPad4,8' then 'iPad mini 3'
        when device_model = 'iPad4,9' then 'iPad Mini 3'
        when device_model = 'iPad5,1' then 'iPad mini 4'
        when device_model = 'iPad5,2' then '4th Gen iPad mini'
        when device_model = 'iPad5,3' then 'iPad Air 2'
        when device_model = 'iPad5,4' then 'iPad Air 2'
        when device_model = 'iPad6,3' then 'iPad Pro'
        when device_model = 'iPad6,4' then 'iPad Pro'
        when device_model = 'iPad6,7' then 'iPad Pro'
        when device_model = 'iPad6,8' then 'iPad Pro'
        when device_model = 'iPad6,11' then 'iPad'
        when device_model = 'iPad6,12' then 'iPad'
        when device_model = 'iPad7,1' then 'iPad Pro 2nd Gen'
        when device_model = 'iPad7,2' then 'iPad Pro 2nd Gen'
        when device_model = 'iPad7,3' then 'iPad Pro 10'
        when device_model = 'iPad7,4' then 'iPad Pro 10'
        when device_model = 'iPad7,5' then 'iPad 6th Gen'
        when device_model = 'iPad7,6' then 'iPad 6th Gen'
        when device_model = 'iPad7,11' then 'iPad 7th Gen 10'
        when device_model = 'iPad7,12' then 'iPad 7th Gen 10'
        when device_model = 'iPad8,1' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,2' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,3' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,4' then 'iPad Pro 11 inch 3rd Gen'
        when device_model = 'iPad8,5' then 'iPad Pro 12'
        when device_model = 'iPad8,6' then 'iPad Pro 12'
        when device_model = 'iPad8,7' then 'iPad Pro 12'
        when device_model = 'iPad8,8' then 'iPad Pro 12'
        when device_model = 'iPad8,9' then 'iPad Pro 11 inch 4th Gen'
        when device_model = 'iPad8,10' then 'iPad Pro 11 inch 4th Gen'
        when device_model = 'iPad8,11' then 'iPad Pro 12'
        when device_model = 'iPad8,12' then 'iPad Pro 12'
        when device_model = 'iPad11,1' then 'iPad mini 5th Gen'
        when device_model = 'iPad11,2' then 'iPad mini 5th Gen'
        when device_model = 'iPad11,3' then 'iPad Air 3rd Gen'
        when device_model = 'iPad11,4' then 'iPad Air 3rd Gen'
        when device_model = 'iPad11,6' then 'iPad 8th Gen'
        when device_model = 'iPad11,7' then 'iPad 8th Gen'
        when device_model = 'iPad12,1' then 'iPad 9th Gen'
        when device_model = 'iPad12,2' then 'iPad 9th Gen'
        when device_model = 'iPad14,1' then 'iPad mini 6th Gen'
        when device_model = 'iPad14,2' then 'iPad mini 6th Gen'
        when device_model = 'iPad13,1' then 'iPad Air 4th Gen'
        when device_model = 'iPad13,2' then 'iPad Air 4th Gen'
        when device_model = 'iPad13,4' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,5' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,6' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,7' then 'iPad Pro 11 inch 5th Gen'
        when device_model = 'iPad13,8' then 'iPad Pro 12'
        when device_model = 'iPad13,9' then 'iPad Pro 12'
        when device_model = 'iPad13,10' then 'iPad Pro 12'
        when device_model = 'iPad13,11' then 'iPad Pro 12'
        when device_model = 'iPad13,16' then 'iPad Air 5th Gen'
        when device_model = 'iPad13,17' then 'iPad Air 5th Gen'
        else device_model
      end as Model,
      'SPARK iOS' as Device
    from
      "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_spark_ios_clickstream_data"
    where
      event_id in ('0.0.0.0.1', '0.0.0.0.4')
      and dt = current_date -1
      and length(client_id) > 1
      and length(event_metadata) > 1
  )
union all
select
  client_id,
  '-' as Network_type,
  '-' as Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    device_model
  ) as Device_model,
  'TAB' as Device,
    cast(dt as date) dt
from
  "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_tab_clickstream_data"
where
  event_id in ('2.7.0.0.0', '2.1.0.0.0')
  and dt = current_date -1
  and length(client_id) > 1
union all
select
  client_id,
  '-' as Network_type,
  '-' as Service_Provider,
  concat(
    concat(device_manufacturer, cast('-' as varchar)),
    device_model
  ) as Device_model,
  'Spark Web' as Device,
    cast(dt as date) dt
from
  "dbo-angels"."clickstream"."db_dbo_clickstream_data_tablename_rtab_clickstream_data"
where
  event_id in ('1.0.0.1.0')
  and dt = current_date -1
  and length(client_id) > 1);


begin transaction;

delete from product_analytics.clickstream_network_details using ck_netwrok_stg
where clickstream_network_details.client_id=ck_netwrok_stg.client_id;

insert into product_analytics.clickstream_network_details
select * from ck_netwrok_stg;

end transaction;

drop table ck_netwrok_stg;

