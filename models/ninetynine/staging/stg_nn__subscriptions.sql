SELECT
	agent_number,
	plan_id,
	EXTRACT(date FROM TIMESTAMP_MICROS(cast(start_date as int64)) AT TIME ZONE "Singapore") as plan_starting_date,
	EXTRACT(date FROM TIMESTAMP_MICROS(cast(end_date as int64)) AT TIME ZONE "Singapore") as plan_expiry_date,
  status
FROM {{ source('ninetynine', 'subscriptions') }}
