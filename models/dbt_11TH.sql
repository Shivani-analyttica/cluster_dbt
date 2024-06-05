-- First, create a CTE to calculate yearly totals
with year_total as (
    select 
        c_customer_id as customer_id,
        c_first_name as customer_first_name,
        c_last_name as customer_last_name,
        c_preferred_cust_flag as customer_preferred_cust_flag,
        c_birth_country as customer_birth_country,
        c_login as customer_login,
        c_email_address as customer_email_address,
        d_year as dyear,
        sum(ss_ext_list_price - ss_ext_discount_amt) as year_total,
        's' as sale_type
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'CUSTOMER') }} as customer
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'STORE_SALES') }} as store_sales on c_customer_sk = ss_customer_sk
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'DATE_DIM') }} as date_dim on ss_sold_date_sk = d_date_sk
    group by 1,2,3,4,5,6,7,8
    union all
    select 
        c_customer_id as customer_id,
        c_first_name as customer_first_name,
        c_last_name as customer_last_name,
        c_preferred_cust_flag as customer_preferred_cust_flag,
        c_birth_country as customer_birth_country,
        c_login as customer_login,
        c_email_address as customer_email_address,
        d_year as dyear,
        sum(ws_ext_list_price - ws_ext_discount_amt) as year_total,
        'w' as sale_type
    from {{ source('SNOWFLAKE_SAMPLE_DATA', 'CUSTOMER') }} as customer
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'WEB_SALES') }} as web_sales on c_customer_sk = ws_bill_customer_sk
    join {{ source('SNOWFLAKE_SAMPLE_DATA', 'DATE_DIM') }} as date_dim on ws_sold_date_sk = d_date_sk
    group by 1,2,3,4,5,6,7,8
),

-- Next, filter the results based on the conditions specified
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
    and t_s_secyear.dyear = 2001 + 1
    and t_w_firstyear.dyear = 2001
    and t_w_secyear.dyear = 2001 + 1
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

