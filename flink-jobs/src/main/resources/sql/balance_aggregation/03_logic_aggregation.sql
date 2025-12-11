-- ==============================================================================
-- WHAT: Aggregation logic for calculating account balances
-- WHY: Computes running total balance per account from transaction stream
-- HOW: Groups by account_id, sums amounts, tracks last transaction time
-- ==============================================================================

INSERT INTO account_balances
SELECT 
    account_id,
    SUM(amount_transferred) as total_balance,
    MAX(ts) as last_updated
FROM transactions
GROUP BY account_id;
