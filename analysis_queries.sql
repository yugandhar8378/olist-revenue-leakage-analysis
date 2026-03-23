-- ================================================
-- OLIST REVENUE LEAKAGE ANALYSIS
-- Author: Yugandhar Reddy
-- Tools: PostgreSQL
-- ================================================

-- ================================================
-- 1. DATABASE SETUP
-- ================================================

-- Tables loaded via Python/SQLAlchemy
-- See data_loading.py for setup script

-- ================================================
-- 2. EXPLORATORY DATA ANALYSIS
-- ================================================

-- Order status distribution
select order_status, 
round(count(order_id) * 100.0 / 
sum(count(order_id)) over(), 2) as pct_contribution
from orders 
group by order_status 
order by pct_contribution desc;

-- Date range of dataset
select 
    min(order_purchase_timestamp) as first_order,
    max(order_purchase_timestamp) as last_order
from orders;

-- ================================================
-- 3. CANCELLATION ANALYSIS (CORE PROJECT)
-- ================================================

-- Cancellation rate by category
select 
    p.product_category_name,
    count(o.order_id) as total_orders,
    sum(case when o.order_status = 'canceled' 
        then 1 else 0 end) as cancelled_orders,
    round(sum(case when o.order_status = 'canceled' 
        then 1 else 0 end) * 100.0 / 
        count(o.order_id), 2) as cancellation_rate
from orders o
join order_items oi on o.order_id = oi.order_id
join products p on oi.product_id = p.product_id
group by p.product_category_name
having count(o.order_id) > 50
order by cancellation_rate desc;

-- Monthly revenue lost to cancellations
with lost_rev as (
    select 
        p.product_category_name,
        to_char(o.order_purchase_timestamp, 'YYYY-MM') as month,
        sum(oi.price) as lost_revenue
    from orders o
    join order_items oi on o.order_id = oi.order_id
    join products p on oi.product_id = p.product_id
    where o.order_status = 'canceled'
    group by p.product_category_name, 
             to_char(o.order_purchase_timestamp, 'YYYY-MM')
)
select 
    product_category_name, month, lost_revenue,
    round(lost_revenue * 100.0 / 
          sum(lost_revenue) over(partition by product_category_name), 
          2) as pct_of_category_loss
from lost_rev
order by product_category_name, month;

-- ================================================
-- 4. CUSTOMER ANALYSIS
-- ================================================

-- High value customers lost between 2017 and 2018
with cus_spent as (
    select o.customer_id, 
           sum(oi.price) as total_spent 
    from orders o 
    join order_items oi on o.order_id = oi.order_id 
    where extract(year from o.order_purchase_timestamp) = 2017
    and o.order_status = 'delivered'
    group by o.customer_id
),
cus_2018 as (
    select distinct customer_id 
    from orders 
    where extract(year from order_purchase_timestamp) = 2018
)
select 
    cs.customer_id,
    cs.total_spent,
    rank() over(order by cs.total_spent desc) as rnk
from cus_spent cs
left join cus_2018 c18 on cs.customer_id = c18.customer_id
where c18.customer_id is null
order by rnk;

-- Customer segmentation by spending
with pct as (
    select c.customer_id, 
           sum(oi.price) as spent, 
           percent_rank() over(order by sum(oi.price) desc) as pct_rank
    from customers c 
    join orders o on c.customer_id = o.customer_id
    join order_items oi on o.order_id = oi.order_id 
    group by c.customer_id 
    having count(o.order_id) >= 2
)
select *,
    case when pct_rank <= 0.2 then 'High Value' 
         when pct_rank <= 0.5 then 'Mid Value' 
         else 'Low Value' end as segment
from pct;

-- ================================================
-- 5. SELLER ANALYSIS
-- ================================================

-- Top seller per month by revenue contribution
with month_seller as (
    select 
        to_char(o.order_purchase_timestamp, 'YYYY-MM') as month, 
        s.seller_id, 
        sum(oi.price) as revenue
    from orders o 
    join order_items oi on o.order_id = oi.order_id 
    join sellers s on oi.seller_id = s.seller_id
    group by to_char(o.order_purchase_timestamp, 'YYYY-MM'), s.seller_id
)
select month, seller_id, revenue, contribution 
from (
    select *, 
        round(revenue::numeric * 100.0 / 
              sum(revenue::numeric) over(partition by month), 2) as contribution,
        row_number() over(partition by month order by revenue desc) as rn 
    from month_seller
) ranked
where rn = 1
order by month;

-- ================================================
-- 6. DELIVERY ANALYSIS
-- ================================================

-- Top 5 states with worst delivery delay
select c.customer_state,
    round(avg(date_part('day', 
        o.order_delivered_customer_date - 
        o.order_estimated_delivery_date))::numeric, 2) as avg_delay_days
from orders o
join customers c on o.customer_id = c.customer_id
where o.order_status = 'delivered'
    and o.order_delivered_customer_date is not null
group by c.customer_state
order by avg_delay_days desc
limit 5;
