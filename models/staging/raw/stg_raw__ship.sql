with 

source as (

    select * from {{ source('raw', 'ship') }}

),

renamed as (

    select
        CAST(orders_id AS float64) AS orders_id,
        shipping_fee,
        shipping_fee_1,
        logcost,
        ship_cost

    from source

    ###WHERE shipping_fee <> shipping_fee_1

)

select * from renamed
