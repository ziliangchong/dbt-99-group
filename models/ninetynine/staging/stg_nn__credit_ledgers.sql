SELECT
	credit_bundle_id,
  ROUND(balance/100, 0) as credit_amount,
	ledger_action
FROM {{ source('ninetynine', 'credit_ledgers') }}
