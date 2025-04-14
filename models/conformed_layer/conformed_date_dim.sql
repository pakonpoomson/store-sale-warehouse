create or replace view `wh_conformed.DATE_DIM_VW` as
select
*,
D_Year as D_Year_AD,
D_Year + 543 as D_Year_BE,
    CASE D_DAY_NAME
        WHEN 'Monday' THEN 'จันทร์'
        WHEN 'Tuesday' THEN 'อังคาร'
        WHEN 'Wednesday' THEN 'พุธ'
        WHEN 'Thursday' THEN 'พฤหัสบดี'
        WHEN 'Friday' THEN 'ศุกร์'
        WHEN 'Saturday' THEN 'เสาร์'
        WHEN 'Sunday' THEN 'อาทิตย์'
        ELSE D_DAY_NAME  -- In case there's an unexpected value
    END AS D_DAY_NAME_TH
from wh_raw.snf_DATE_DIM