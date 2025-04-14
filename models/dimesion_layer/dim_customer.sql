create or replace view `wh_dimension.DIM_CUSTOMER_VW` as
select
C_CUSTOMER_SK,
C_FIRST_NAME,
C_LAST_NAME,
C_SALUTATION,
C_EMAIL_ADDRESS,
C_BIRTH_COUNTRY
from wh_conformed.CUSTOMER_VW