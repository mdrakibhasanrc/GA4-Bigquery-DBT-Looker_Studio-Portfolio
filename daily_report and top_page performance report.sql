SELECT  
   *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`;

-- Calculate total users

SELECT  
 
   count(user_pseudo_id) as total_user

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Calculate sessions
SELECT  
 
   count(concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key='ga_session_id'))) as total_session

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- Calculate views

SELECT  

  countif(event_name in ('page_view','screen_view'))  as page_view
   
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;


-- daily performance report 

SELECT  
    parse_date('%Y%m%d', event_date) as event_day,
    count(distinct user_pseudo_id) as total_user,
    count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key='ga_session_id'))) as total_session,
    countif(event_name in ('page_view','screen_view'))  as page_view,
    round((count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key='ga_session_id')))/count(distinct user_pseudo_id)),0) as session_per_user,

    round((countif(event_name in ('page_view','screen_view'))/count(distinct user_pseudo_id)),0) as view_by_user

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by event_date
order by total_user desc;


-- top 10  pages report

SELECT  
    (select value.string_value from unnest(event_params) where key='page_location') as page,
    count(*) as total_views,
    count(distinct user_pseudo_id) as total_user,
    round(count(*)/ count(distinct user_pseudo_id),0) as view_per_user
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by page
order by total_views desc
limit 10;

