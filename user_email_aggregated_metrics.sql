-- Retrieve the latest known country for each account based on sessions
WITH session_country AS (
 SELECT
   asn.account_id,
   sp.country,
   s.date AS session_date,
   ROW_NUMBER() OVER (PARTITION BY asn.account_id ORDER BY s.date DESC) AS rn
 FROM `data-analytics-mate.DA.account_session`  AS asn
 JOIN `data-analytics-mate.DA.session`          AS s   USING (ga_session_id)
 JOIN `data-analytics-mate.DA.session_params`   AS sp  USING (ga_session_id)
 WHERE sp.country IS NOT NULL
),
account_country AS (
 SELECT account_id, country
 FROM session_country
 WHERE rn = 1
),

-- Determine the account creation date
account_first_session AS (
 SELECT
   asn.account_id,
   MIN(s.date) AS created_date
 FROM `data-analytics-mate.DA.account_session` asn
 JOIN `data-analytics-mate.DA.session` s USING (ga_session_id)
 GROUP BY asn.account_id
),

-- Create a dimensional base for accounts
accounts_dim AS (
 SELECT
   afs.created_date             AS date,      -- Account creation date (for aggregation)
   ac.country                   AS country,   -- Last known country
   a.send_interval,                             -- Sending frequency category
   a.is_verified,                            -- Account verification status
   a.is_unsubscribed,                           -- Subscription status
   a.id                         AS account_id
 FROM `data-analytics-mate.DA.account` a
 JOIN account_country       ac  ON ac.account_id  = a.id
 JOIN account_first_session afs ON afs.account_id = a.id
),

-- Aggregate account creation events based on dimensions
accounts_metrics AS (
 SELECT
   date, country, send_interval, is_verified, is_unsubscribed,
   COUNT(DISTINCT account_id) AS account_cnt, -- Count of new accounts created
   0 AS sent_msg, 0 AS open_msg, 0 AS visit_msg -- Placeholder for email metrics
 FROM accounts_dim
 GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Create the base for email analysis
emails_base AS (
 SELECT
   ad.date + INTERVAL es.sent_date DAY AS sent_day, -- Calculate the actual sending date
   ad.country,                          -- Join account dimensions
   ad.send_interval,                              
   ad.is_verified,                      
   ad.is_unsubscribed, 
   es.id_message,
   es.id_account
 FROM `data-analytics-mate.DA.email_sent` es
 JOIN accounts_dim ad
   ON ad.account_id = es.id_account
),

-- Enrich email data with open and visit status
emails_dim AS (
 SELECT
   eb.sent_day        AS date, -- Use the email sent date for aggregation
   eb.country,
   eb.send_interval,
   eb.is_verified,
   eb.is_unsubscribed,
   eb.id_message,
   eo.id_message AS id_message_open, -- Present if the message was opened
   ev.id_message AS id_message_visit -- Present if the message was visited (clicked)
 FROM emails_base eb
 LEFT JOIN `data-analytics-mate.DA.email_open`  eo
   ON eo.id_message = eb.id_message
 LEFT JOIN `data-analytics-mate.DA.email_visit` ev
   ON ev.id_message = eb.id_message
),

-- Aggregate email metrics based on dimensions
emails_metrics AS (
 SELECT
   date, country, send_interval, is_verified, is_unsubscribed,
   0 AS account_cnt, -- Placeholder for account creation count
   COUNT(DISTINCT id_message)       AS sent_msg,
   COUNT(DISTINCT id_message_open)  AS open_msg,
   COUNT(DISTINCT id_message_visit) AS visit_msg
 FROM emails_dim
 GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Combine account creation and email metrics into a single dataset and aggregate totals
combined AS (
 SELECT * FROM accounts_metrics
 UNION ALL
 SELECT * FROM emails_metrics
),
aggregated AS (
 SELECT
   date, country, send_interval, is_verified, is_unsubscribed,
   SUM(account_cnt) AS account_cnt,
   SUM(sent_msg)    AS sent_msg,
   SUM(open_msg)    AS open_msg,
   SUM(visit_msg)   AS visit_msg
 FROM combined
 GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Calculate total account and sent message counts per country
with_totals AS (
 SELECT
   a.*,
   SUM(a.account_cnt) OVER (PARTITION BY a.country) AS total_country_account_cnt,
   SUM(a.sent_msg)    OVER (PARTITION BY a.country) AS total_country_sent_cnt
 FROM aggregated a
),

-- Rank countries based on total accounts and total sent messages
with_ranks AS (
 SELECT
   wt.*,
   DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
   DENSE_RANK() OVER (ORDER BY total_country_sent_cnt    DESC) AS rank_total_country_sent_cnt
 FROM with_totals wt
)

-- Final selection, filtering results to the Top 10 countries by accounts OR sent messages
SELECT
 date,
 country,
 send_interval,
 is_verified,
 is_unsubscribed,
 account_cnt,
 sent_msg,
 open_msg,
 visit_msg,
 total_country_account_cnt,
 total_country_sent_cnt,
 rank_total_country_account_cnt,
 rank_total_country_sent_cnt
FROM with_ranks
WHERE
 rank_total_country_account_cnt <= 10
 OR
 rank_total_country_sent_cnt    <= 10
ORDER BY country, date, send_interval, is_verified, is_unsubscribed;
