SELECT
    t.*,
    COALESCE(ka1.name, 'Unknown') AS account_friendly_name,
    COALESCE(ka2.name, 'Unknown') AS link_friendly_name
FROM 
    transactions t
LEFT JOIN 
    known_accounts ka1 ON t.account = ka1.account
LEFT JOIN 
    known_accounts ka2 ON t.link = ka2.account
WHERE 
    t.tstamp >= NOW() - INTERVAL '1 day'
ORDER BY 
    ABS(t.amount) DESC;