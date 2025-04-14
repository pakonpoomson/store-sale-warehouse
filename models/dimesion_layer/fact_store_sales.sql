create or replace view `wh_dimension.FACT_STORE_SALES_VW` as
select
SS_SOLD_DATE_SK
SS_SOLD_TIME_SK,
SS_ITEM_SK,
SS_CUSTOMER_SK,
SS_CDEMO_SK,
SS_NET_PAID
from wh_conformed.STORE_SALES_VW