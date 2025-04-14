
create or replace view `wh_dimension.DIM_ITEM_VW` as
select
I_ITEM_SK,
I_PRODUCT_NAME,
I_ITEM_DESC,
I_UNITS,
I_CATEGORY,
I_BRAND,
I_MANUFACT,
from wh_conformed.ITEM_VW
