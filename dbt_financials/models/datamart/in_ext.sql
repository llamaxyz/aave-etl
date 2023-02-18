select 
  *
from {{ source('financials_data_lake','internal_external_addresses')}}

