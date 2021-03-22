with all_active_subscriptions as ( /*establish all unexpired subsciptions*/
SELECT
  *
FROM {{ ref('stg_nn__subscriptions') }}
WHERE plan_starting_date <= CURRENT_DATE /*catch all subscriptions as long as they have started*/
	AND plan_expiry_date >= CURRENT_DATE /*catch all subscriptions as long as they have not past expiration date*/
)
select * from all_active_subscriptions
