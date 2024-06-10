{{ config(
    materialized='table',
    alias='CROSS_ITEMS'
) }}

-- Combine filtering of DATE_DIM and ITEM tables
with filtered_dates as (
    select
        d_date_sk,
        d_year,
        d_week_seq,
        d_moy,
        d_dom
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'DATE_DIM') }}
    where d_year in (1999, 2000)
),

item_cte as (
    select
        i_item_sk,
        i_brand_id,
        i_class_id,
        i_category_id
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'ITEM') }}
),

store_sales_cte as (
    select
        ss_ticket_number,
        ss_item_sk,
        ss_sold_date_sk,
        ss_quantity,
<<<<<<< HEAD
        ss_list_price,
        iss.i_brand_id,
        iss.i_class_id,
        iss.i_category_id
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'STORE_SALES') }} store_sales
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'ITEM') }} iss on store_sales.ss_item_sk = iss.i_item_sk
    join filtered_dates d1 on store_sales.ss_sold_date_sk = d1.d_date_sk
=======
        ss_list_price
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'STORE_SALES') }}
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
),

catalog_sales_cte as (
    select
        cs_order_number,
        cs_item_sk,
        cs_sold_date_sk,
        cs_quantity,
<<<<<<< HEAD
        cs_list_price,
        ics.i_brand_id,
        ics.i_class_id,
        ics.i_category_id
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'CATALOG_SALES') }} catalog_sales
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'ITEM') }} ics on catalog_sales.cs_item_sk = ics.i_item_sk
    join filtered_dates d2 on catalog_sales.cs_sold_date_sk = d2.d_date_sk
=======
        cs_list_price
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'CATALOG_SALES') }}
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
),

web_sales_cte as (
    select
        ws_order_number,
        ws_item_sk,
        ws_sold_date_sk,
        ws_quantity,
<<<<<<< HEAD
        ws_list_price,
        iws.i_brand_id,
        iws.i_class_id,
        iws.i_category_id
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'WEB_SALES') }} web_sales
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'ITEM') }} iws on web_sales.ws_item_sk = iws.i_item_sk
    join filtered_dates d3 on web_sales.ws_sold_date_sk = d3.d_date_sk
=======
        ws_list_price
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'WEB_SALES') }}
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
),

cross_items as (
    select distinct
<<<<<<< HEAD
        i_item_sk
    from item_cte item
    join (
        select
            i_brand_id,
            i_class_id,
            i_category_id
        from store_sales_cte
        intersect
        select
            i_brand_id,
            i_class_id,
            i_category_id
        from catalog_sales_cte
        intersect
        select
            i_brand_id,
            i_class_id,
            i_category_id
        from web_sales_cte
=======
        item.i_item_sk
    from item_cte item
    join (
        select
            iss.i_brand_id,
            iss.i_class_id,
            iss.i_category_id
        from store_sales_cte store_sales
        join item_cte iss on store_sales.ss_item_sk = iss.i_item_sk
        join date_dim_cte d1 on store_sales.ss_sold_date_sk = d1.d_date_sk
        where d1.d_year between 1999 and 2001
        intersect
        select
            ics.i_brand_id,
            ics.i_class_id,
            ics.i_category_id
        from catalog_sales_cte catalog_sales
        join item_cte ics on catalog_sales.cs_item_sk = ics.i_item_sk
        join date_dim_cte d2 on catalog_sales.cs_sold_date_sk = d2.d_date_sk
        where d2.d_year between 1999 and 2001
        intersect
        select
            iws.i_brand_id,
            iws.i_class_id,
            iws.i_category_id
        from web_sales_cte web_sales
        join item_cte iws on web_sales.ws_item_sk = iws.i_item_sk
        join date_dim_cte d3 on web_sales.ws_sold_date_sk = d3.d_date_sk
        where d3.d_year between 1999 and 2001
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
    ) x on x.i_brand_id = item.i_brand_id
       and x.i_class_id = item.i_class_id
       and x.i_category_id = item.i_category_id
),

avg_sales as (
    select
        avg(quantity * list_price) as average_sales
    from (
        select
            ss_quantity as quantity,
            ss_list_price as list_price
        from store_sales_cte
        union all
        select
            cs_quantity as quantity,
            cs_list_price as list_price
        from catalog_sales_cte
        union all
        select
            ws_quantity as quantity,
            ws_list_price as list_price
        from web_sales_cte
    )
),

-- Common logic for this_year and last_year
sales_data as (
    select
        'store' as channel,
        item.i_brand_id,
        item.i_class_id,
        item.i_category_id,
        sum(store_sales.ss_quantity * store_sales.ss_list_price) as sales,
<<<<<<< HEAD
        count(store_sales.ss_ticket_number) as number_sales,
        d1.d_year,
        d1.d_week_seq,
        d1.d_moy,
        d1.d_dom
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'STORE_SALES') }} store_sales
    join item_cte item on store_sales.ss_item_sk = item.i_item_sk
    join filtered_dates d1 on store_sales.ss_sold_date_sk = d1.d_date_sk
    where store_sales.ss_item_sk in (select i_item_sk from cross_items)
    group by item.i_brand_id, item.i_class_id, item.i_category_id, d1.d_year, d1.d_week_seq, d1.d_moy, d1.d_dom
),

this_year as (
    select
        channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sales,
        number_sales
    from sales_data
    where d_year = 2000 and d_week_seq = (select d_week_seq from filtered_dates where d_year = 2000 and d_moy = 12 and d_dom = 11)
    having sales > (select average_sales from avg_sales)
=======
        count(store_sales.ss_ticket_number) as number_sales -- Using surrogate key to count sales
    from store_sales_cte store_sales
    join item_cte item on store_sales.ss_item_sk = item.i_item_sk
    join date_dim_cte d1 on store_sales.ss_sold_date_sk = d1.d_date_sk
    where store_sales.ss_item_sk in (select i_item_sk from cross_items)
      and d1.d_week_seq = (select d_week_seq from date_dim_cte where d_year = 2000 and d_moy = 12 and d_dom = 11)
    group by item.i_brand_id, item.i_class_id, item.i_category_id
    having sum(store_sales.ss_quantity * store_sales.ss_list_price) > (select average_sales from avg_sales)
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
),

last_year as (
    select
<<<<<<< HEAD
        channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sales,
        number_sales
    from sales_data
    where d_year = 1999 and d_week_seq = (select d_week_seq from filtered_dates where d_year = 1999 and d_moy = 12 and d_dom = 11)
    having sales > (select average_sales from avg_sales)
=======
        'store' as channel,
        item.i_brand_id,
        item.i_class_id,
        item.i_category_id,
        sum(store_sales.ss_quantity * store_sales.ss_list_price) as sales,
        count(store_sales.ss_ticket_number) as number_sales -- Using surrogate key to count sales
    from store_sales_cte store_sales
    join item_cte item on store_sales.ss_item_sk = item.i_item_sk
    join date_dim_cte d2 on store_sales.ss_sold_date_sk = d2.d_date_sk
    where store_sales.ss_item_sk in (select i_item_sk from cross_items)
      and d2.d_week_seq = (select d_week_seq from date_dim_cte where d_year = 1999 and d_moy = 12 and d_dom = 11)
    group by item.i_brand_id, item.i_class_id, item.i_category_id
    having sum(store_sales.ss_quantity * store_sales.ss_list_price) > (select average_sales from avg_sales)
>>>>>>> 3d564a2422e2ec7e9ab4cd101b7703b95074736d
)

-- Final query
select
    this_year.channel as ty_channel,dbt
    this_year.i_brand_id as ty_brand,
    this_year.i_class_id as ty_class,
    this_year.i_category_id as ty_category,
    this_year.sales as ty_sales,
    this_year.number_sales as ty_number_sales,
    last_year.channel as ly_channel,
    last_year.i_brand_id as ly_brand,
    last_year.i_class_id as ly_class,
    last_year.i_category_id as ly_category,
    last_year.sales as ly_sales,
    last_year.number_sales as ly_number_sales
from this_year
join last_year on this_year.i_brand_id = last_year.i_brand_id
   and this_year.i_class_id = last_year.i_class_id
   and this_year.i_category_id = last_year.i_category_id
order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
