CREATE OR REPLACE VIEW 
  `$VIEW_PROJECT_ID.$VIEW_DATASET.$VIEW_NAME` AS

WITH carbon AS (
  SELECT
        billing_account_id,
        usage_month,
        service.id AS service_id,
        project.number AS project_number,
        location.location AS location_location,
        ROUND(carbon_footprint_total_kgCO2e.location_based, 3) AS carbon_footprint_kgCO2e, -- Kept the previous name of the field for retrocompatibility with the dashboard downstream
        ROUND(carbon_footprint_total_kgCO2e.market_based, 3) AS carbon_footprint_kgCO2e_market_based,
        ROUND(carbon_footprint_kgCO2e.scope1, 3) AS scope1,
        STRUCT(
          ROUND(carbon_footprint_kgCO2e.scope2.location_based) AS location_based, 
          ROUND(carbon_footprint_kgCO2e.scope2.market_based) AS market_based
          ) AS scope2,
        ROUND(carbon_footprint_kgCO2e.scope3, 3) AS scope3
    FROM 
      `$CARBON_PROJECT_ID.$CARBON_DATASET.$CARBON_TABLE`
),

billing AS (
  SELECT
    billing_account_id,
    service.id AS service_id,
    service.description AS service_description,
    project_id,
    project_number,
    project_name,
    location.location AS location_location,
    location.country AS location_country,
    location.region AS location_region,
    location.zone AS location_zone,
    billing_invoice_month,
    ROUND(SUM(cost), 3) AS monthly_cost,
    ROUND(SUM(net_cost), 3) AS monthly_net_cost,
  FROM (
    SELECT
      billing_account_id,
      service,
      project.id AS project_id,
      project.number AS project_number,
      project.name AS project_name,
      location,
      DATE(PARSE_DATE("%Y%m",
        invoice.month)) AS billing_invoice_month,
      cost,
      COALESCE((
        SELECT
          SUM(x.amount)
        FROM
          UNNEST(billing.credits) x),
        0) + cost AS net_cost
    FROM
      `$BILLING_PROJECT_ID.$BILLING_DATASET.$BILLING_TABLE` AS billing
    WHERE
      currency = "USD"
    )
  GROUP BY
    billing_account_id,
    service_id,
    service_description,
    project_id,
    project_number,
    project_name,
    location_location,
    location_country,
    location_region,
    location_zone,
    billing_invoice_month
  ),

label_asset AS (
  SELECT
    number,
    ARRAY_AGG(STRUCT(key AS key,
        value AS value)) AS labels
  FROM (
    SELECT
      DISTINCT project.number AS number,
      labels.key,
      labels.value
    FROM
      `$BILLING_PROJECT_ID.$BILLING_DATASET.$BILLING_TABLE`,
      UNNEST(project.labels) labels
    )
  GROUP BY
    1
),

billing_usage AS (
  SELECT
    billing_account_id,
    service_id,
    project_number,
    location_location,
    billing_invoice_month,
    ARRAY_AGG(STRUCT(unit AS unit,
        amount AS amount)) AS monthly_usage_volume
  FROM (
    SELECT
      billing_account_id,
      service.id AS service_id,
      project.number AS project_number,
      location.location AS location_location,
      DATE(PARSE_DATE("%Y%m",
          invoice.month)) AS billing_invoice_month,
      usage.unit AS unit,
      ROUND(SUM(usage.amount), 3) AS amount
    FROM
      `$BILLING_PROJECT_ID.$BILLING_DATASET.$BILLING_TABLE`
    WHERE
      currency = "USD" 
    GROUP BY
      billing_account_id,
      service.id,
      project.number,
      location_location,
      billing_invoice_month,
      usage.unit)
  GROUP BY
    billing_account_id,
    service_id,
    project_number,
    location_location,
    billing_invoice_month 
)

SELECT 
  b.billing_account_id,
  STRUCT(
    b.service_id AS id,
    b.service_description AS description) AS service,
  STRUCT(
    b.project_id AS id,
    b.project_number AS number,
    b.project_name AS name
  ) AS project,
  STRUCT(
    b.location_location AS location,
    b.location_country AS country,
    b.location_region AS region,
    b.location_zone AS zone
  ) AS location,
  b.billing_invoice_month AS usage_month,
  b.monthly_cost,
  b.monthly_net_cost, 
  c.carbon_footprint_kgCO2e,
  c.carbon_footprint_kgCO2e_market_based,
  c.scope1,
  c.scope2,
  c.scope3,
  l.labels,
  billing_usage.monthly_usage_volume
FROM billing AS b
INNER JOIN carbon AS c
  ON
    c.project_number = b.project_number
    AND c.billing_account_id = b.billing_account_id
    AND c.usage_month = b.billing_invoice_month
    AND c.location_location = b.location_location
    AND c.service_id = b.service_id
LEFT JOIN label_asset AS l
  ON
    b.project_number = l.number
LEFT JOIN billing_usage 
  ON
    billing_usage.project_number = b.project_number
    AND billing_usage.billing_account_id = b.billing_account_id
    AND billing_usage.billing_invoice_month = b.billing_invoice_month
    AND billing_usage.location_location = b.location_location
    AND billing_usage.service_id = b.service_id
    