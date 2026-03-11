-- Lab03 investigation: find the culprit and print md5(lower(trim(full_name)))
SET search_path = lab03, public;

WITH day0 AS (
  SELECT MIN(ts::date) AS d FROM purchases
),
-- GOLDEN WALRUS intro talk code
gw AS (
  SELECT session_code FROM sessions WHERE session_name ILIKE 'GOLDEN WALRUS%' LIMIT 1
),
-- People present at GOLDEN WALRUS between 13:00–13:15
suspects AS (
  SELECT DISTINCT sw.badge_uid
  FROM swipes sw, day0 d, gw
  WHERE sw.session_code = gw.session_code
    AND sw.ts >= (d.d::timestamp + time '13:00')
    AND sw.ts <  (d.d::timestamp + time '13:15')
),
-- Cafeteria purchases ~2h later (from 15:00 to end-of-day)
p AS (
  SELECT pu.*
  FROM purchases pu, day0 d
  WHERE pu.location = 'cafeteria'
    AND pu.ts >= (d.d::timestamp + time '15:00')
    AND pu.ts <  (d.d::timestamp + time '23:59:59.999')
),
-- Mark groups split by any non-coffee purchase per person
runs AS (
  SELECT
    badge_uid,
    product,
    ts,
    SUM(CASE WHEN product <> 'Coffee' THEN 1 ELSE 0 END)
      OVER (PARTITION BY badge_uid ORDER BY ts) AS grp
  FROM p
),
-- Count consecutive Coffee purchases per person
coffee_runs AS (
  SELECT badge_uid, grp, COUNT(*) AS run_len,
         MIN(ts) AS first_ts, MAX(ts) AS last_ts
  FROM runs
  WHERE product = 'Coffee'
  GROUP BY badge_uid, grp
),
-- Candidates: 3+ coffees in a row after 15:00
candidates AS (
  SELECT DISTINCT badge_uid FROM coffee_runs WHERE run_len >= 3
)
SELECT
  pe.full_name,
  pe.badge_uid,
  cr.run_len AS max_consecutive_coffees,
  cr.first_ts,
  cr.last_ts,
  md5(lower(trim(pe.full_name))) AS culprit_checksum
FROM people pe
JOIN suspects s ON s.badge_uid = pe.badge_uid
JOIN LATERAL (
  SELECT run_len, first_ts, last_ts
  FROM coffee_runs cr2
  WHERE cr2.badge_uid = pe.badge_uid
  ORDER BY run_len DESC, last_ts DESC
  LIMIT 1
) cr ON true
JOIN candidates c ON c.badge_uid = pe.badge_uid
ORDER BY cr.run_len DESC, cr.last_ts DESC
LIMIT 1;
