-- Query 1: Noise complaint hotspots by ZIP code
-- Finds ZIP codes with at least 100 noise complaints that have geolocation data
SELECT incident_zip,
       COUNT(*) AS noise_requests_with_geo
FROM service_requests
WHERE has_geo = TRUE  -- Only include records with valid geolocation
  AND complaint_type ILIKE 'Noise%'  -- Filter for noise complaints
GROUP BY incident_zip
HAVING COUNT(*) >= 100  -- Only show ZIPs with 100+ noise complaints
ORDER BY noise_requests_with_geo DESC;


-- Query 2: Service requests by year, month, and borough
-- Shows total requests for each year-month-borough combination
SELECT created_year,
       created_month,
       borough,
       COUNT(*) AS total_requests
FROM service_requests
GROUP BY created_year, created_month, borough
ORDER BY created_year, created_month, borough;


-- Query 3: Total requests by borough
-- Aggregates all requests per borough to show which areas are busiest
SELECT borough,
       COUNT(*) AS total_requests
FROM service_requests
GROUP BY borough
ORDER BY total_requests DESC;


-- Query 4: Top 5 complaint types per borough
-- Uses window functions to rank complaint types within each borough
WITH counts AS (
    SELECT borough,
           complaint_type,
           COUNT(*) AS total_requests,
           ROW_NUMBER() OVER (
               PARTITION BY borough  -- Separate ranking for each borough
               ORDER BY COUNT(*) DESC
           ) AS rn
    FROM service_requests
    GROUP BY borough, complaint_type
)
SELECT borough, complaint_type, total_requests
FROM counts
WHERE rn <= 5  -- Only include top 5 complaints per borough
ORDER BY borough, total_requests DESC;