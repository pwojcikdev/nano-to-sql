SELECT
    t.account, 
    COALESCE(ka.name, 'Unknown') AS friendly_name,
    SUM(ABS(t.amount)) as total_amount
FROM
    transactions t
LEFT JOIN
    known_accounts ka ON t.account = ka.account
WHERE 
    t.tstamp >= NOW() - INTERVAL '1 day'
GROUP BY
    t.account, ka.name
ORDER BY
    total_amount DESC;