with

    source as (select * from {{ source("raw", "product") }}),

    renamed as (

        select products_id,
        CAST(purchse_price as float64) as purchase_price from source

    )

select *
from renamed
