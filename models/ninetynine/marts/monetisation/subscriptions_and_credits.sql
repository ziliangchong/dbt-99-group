with subscriptions as (

    select * from {{ ref('current_subscriptions') }}

),

credits as (

    select * from {{ ref('credit_wallet') }}

),

final as (

    select
        s.*,
        c.free_credits,
        c.paid_credits,
        c.free_credits_spent,
        c.paid_credits_spent,
        c.free_credits_remaining,
        c.paid_credits_remaining,
        c.total_credits_remaining

    from subscriptions s

    left join credits c on s.agent_number = c.agent_number

)

select * from final
