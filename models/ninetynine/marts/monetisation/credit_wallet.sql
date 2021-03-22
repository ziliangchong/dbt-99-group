with active_credits_bundles as ( /*find all unexpired credit bundles*/
SELECT
	bundle_id,
	bundle_price,
  agent_number
FROM {{ ref('stg_nn__credit_bundles') }}
WHERE status = 'active'
AND bundle_expiry_date > CURRENT_DATE
AND bundle_starting_date <= CURRENT_DATE
)
,
active_credits_ledgers as ( /*linking credit transactions (credit and debit) to unexpired bundles. transactions on expired bundles can be ignored*/
SELECT
	b.*,
	l.credit_amount,
	l.ledger_action
FROM active_credits_bundles b
LEFT JOIN {{ ref('stg_nn__credit_ledgers') }} l
	ON b.bundle_id = l.credit_bundle_id
)
,
spending as ( /*calculate credit spending, split into free and paid credits*/
SELECT
  agent_number,
  SUM(CASE WHEN bundle_price = 0 and ledger_action in ('credit', 'transfer_to') THEN credit_amount ELSE 0 END) as free_credits, /*take transfer credits into acct as well*/
	SUM(CASE WHEN bundle_price <> 0 and ledger_action in ('credit', 'transfer_to') THEN credit_amount ELSE 0 END) as paid_credits,
	SUM(CASE WHEN bundle_price = 0 and ledger_action in ('debit', 'transfer_from') THEN credit_amount * -1 ELSE 0 END) as free_credits_spent, /*times negative one because debit is in negative*/
	SUM(CASE WHEN bundle_price <> 0 and ledger_action in ('debit', 'transfer_from')  THEN credit_amount * -1 ELSE 0 END) as paid_credits_spent,/*times negative one because debit is in negative*/
FROM active_credits_ledgers
GROUP BY 1
)
SELECT
  *,
  free_credits - free_credits_spent as free_credits_remaining, /*calculate how much credits remaining in each tier*/
	paid_credits - paid_credits_spent as paid_credits_remaining,
  free_credits + paid_credits - free_credits_spent - paid_credits_spent as total_credits_remaining
FROM spending
