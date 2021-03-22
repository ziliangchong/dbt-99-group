SELECT
	id as bundle_id,
	bundle_price,
	agent_number,
	EXTRACT(date FROM TIMESTAMP_MICROS(cast(start_at as int64)) AT TIME ZONE "Singapore") as bundle_starting_date,
	EXTRACT(date FROM TIMESTAMP_MICROS(cast(expired_at as int64)) AT TIME ZONE "Singapore") as bundle_expiry_date,
  status
FROM {{ source('ninetynine', 'credit_bundles') }}
