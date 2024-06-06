-- CTE for the CUSTOMER table
with customer_cte as (
   select
       c_customer_sk,
       c_customer_id,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_country,
       c_login,
       c_email_address
   from {{ source('SNOWFLAKE_SAMPLE_DATA', 'CUSTOMER') }}
),
-- CTE for the STORE_SALES table
store_sales_cte as (
   select
       ss_customer_sk,
       ss_sold_date_sk,
       ss_ext_list_price,
       ss_ext_discount_amt
   from {{ source('SNOWFLAKE_SAMPLE_DATA', 'STORE_SALES') }}
),
-- CTE for the WEB_SALES table
web_sales_cte as (
   select
       ws_bill_customer_sk,
       ws_sold_date_sk,
       ws_ext_list_price,
       ws_ext_discount_amt
   from {{ source('SNOWFLAKE_SAMPLE_DATA', 'WEB_SALES') }}
),
-- CTE for the DATE_DIM table
date_dim_cte as (
   select
       d_date_sk,
       d_year
   from {{ source('SNOWFLAKE_SAMPLE_DATA', 'DATE_DIM') }}
),
-- CTE to calculate yearly totals for store sales
store_sales_yearly as (
   select
       customer.c_customer_id as customer_id,
       customer.c_first_name as customer_first_name,
       customer.c_last_name as customer_last_name,
       customer.c_preferred_cust_flag as customer_preferred_cust_flag,
       customer.c_birth_country as customer_birth_country,
       customer.c_login as customer_login,
       customer.c_email_address as customer_email_address,
       date_dim.d_year as dyear,
       sum(store_sales.ss_ext_list_price - store_sales.ss_ext_discount_amt) as year_total,
       's' as sale_type
   from customer_cte as customer
   join store_sales_cte as store_sales on customer.c_customer_sk = store_sales.ss_customer_sk
   join date_dim_cte as date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
   group by customer.c_customer_id, customer.c_first_name, customer.c_last_name, customer.c_preferred_cust_flag,
            customer.c_birth_country, customer.c_login, customer.c_email_address, date_dim.d_year
),
-- CTE to calculate yearly totals for web sales
web_sales_yearly as (
   select
       customer.c_customer_id as customer_id,
       customer.c_first_name as customer_first_name,
       customer.c_last_name as customer_last_name,
       customer.c_preferred_cust_flag as customer_preferred_cust_flag,
       customer.c_birth_country as customer_birth_country,
       customer.c_login as customer_login,
       customer.c_email_address as customer_email_address,
       date_dim.d_year as dyear,
       sum(web_sales.ws_ext_list_price - web_sales.ws_ext_discount_amt) as year_total,
       'w' as sale_type
   from customer_cte as customer
   join web_sales_cte as web_sales on customer.c_customer_sk = web_sales.ws_bill_customer_sk
   join date_dim_cte as date_dim on web_sales.ws_sold_date_sk = date_dim.d_date_sk
   group by customer.c_customer_id, customer.c_first_name, customer.c_last_name, customer.c_preferred_cust_flag,
            customer.c_birth_country, customer.c_login, customer.c_email_address, date_dim.d_year
),
-- Union CTE to combine both store and web sales yearly totals
year_total as (
   select * from store_sales_yearly
   union all
   select * from web_sales_yearly
),
-- CTE to filter the results based on the conditions specified
filtered_results as (
   select
       t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_preferred_cust_flag
   from year_total as t_s_firstyear
   join year_total as t_s_secyear on t_s_secyear.customer_id = t_s_firstyear.customer_id
   join year_total as t_w_firstyear on t_s_firstyear.customer_id = t_w_firstyear.customer_id
   join year_total as t_w_secyear on t_s_firstyear.customer_id = t_w_secyear.customer_id
   where t_s_firstyear.sale_type = 's'
   and t_w_firstyear.sale_type = 'w'
   and t_s_secyear.sale_type = 's'
   and t_w_secyear.sale_type = 'w'
   and t_s_firstyear.dyear = 2001
   and t_s_secyear.dyear = 2002
   and t_w_firstyear.dyear = 2001
   and t_w_secyear.dyear = 2002
   and t_s_firstyear.year_total > 0
   and t_w_firstyear.year_total > 0
   and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
> case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
)
-- Finally, select and order the filtered results
select
   customer_id,
   customer_first_name,
   customer_last_name,
   customer_preferred_cust_flag
from filtered_results