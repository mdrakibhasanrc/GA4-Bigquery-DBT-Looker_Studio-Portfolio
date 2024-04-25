---- Total Report User Metric and dimension by event date

with user_metric as 

(SELECT 
     parse_date('%Y%m%d',event_date) as event_date,

     count(distinct user_pseudo_id) as total_user,

     count(distinct case when (select value.int_value from unnest (event_params) where key='ga_session_number') =1 then 
     user_pseudo_id else null end) as new_user,

     count(distinct case when (select value.int_value from unnest (event_params) where key='engagement_time_msec') > 0 or 
      (select value.string_value from unnest (event_params) where key='session_engaged')='1' then user_pseudo_id else null     end ) as active_user,

    round(count(distinct case when (select value.int_value from unnest (event_params) where key='ga_session_number') =1 then 
    user_pseudo_id else null end) / count(distinct user_pseudo_id),2) as _new_user_percentage,

    count(distinct case when (select value.int_value from unnest (event_params) where key='ga_session_number') =1 then concat(
    user_pseudo_id,(select value.int_value from unnest (event_params) where key='ga_session_id')) else null end) as 
     new_session,

    count(distinct case when (select value.int_value from unnest (event_params) where key='ga_session_number') =1 then concat(
    user_pseudo_id,(select value.int_value from unnest (event_params) where key='ga_session_id')) else null end) / 
    count(distinct concat(user_pseudo_id,(select value.int_value from unnest (event_params) where key='ga_session_id'))) as 
     pct_new_session,

    count(distinct concat(user_pseudo_id,(select value.int_value from unnest (event_params) where key='ga_session_id'))) / 
    count(distinct user_pseudo_id) as num_session_per_user,

    count(event_name)/count(distinct user_pseudo_id) as event_count_user

 FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
 group by event_date
 order by total_user desc)
 
 select
      event_date,
      new_user,
      active_user,
      _new_user_percentage,
      new_session,
      pct_new_session,
      num_session_per_user,
      event_count_user
  from user_metric;



 -- User Type
  SELECT 
     case
        when (select value.int_value from unnest (event_params) where event_name='session_start' and 
        key='ga_session_id')=1 then 'New Users'
        when (select value.int_value from unnest (event_params) where event_name='session_start' and 
        key='ga_session_id')>1 then 'Returning User'
        else null end as  user_type,
        count(distinct user_pseudo_id) as total_user
 FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
 group by user_type
 having user_type is not null;