SELECT
  billing.billing_account_id,
  STRUCT(billing.service_id AS id,
    billing.service_description AS description) AS service,
  STRUCT(billing.project_id AS id,
    billing.project_number AS number,
    billing.project_name AS name) AS project,
  STRUCT(billing.location_location AS location,
    billing.location_country AS country,
    billing.location_region AS region,
    billing.location_zone AS zone) AS location,
  billing.billing_invoice_month AS usage_month,
  billing.monthly_cost,
  billing.monthly_net_cost, 
  carbon.carbon_footprint_kgCO2e,
  label_asset.labels,
  billing_usage.monthly_usage_volume
FROM (
  SELECT
    billing_account_id,
    service.id AS service_id,
    service.description AS service_description,
    project.id AS project_id,
    project.number AS project_number,
    project.name AS project_name,
    location.location AS location_location,
    location.country AS location_country,
    location.region AS location_region,
    location.zone AS location_zone,
    DATE(PARSE_DATE("%Y%m",
        invoice.month)) AS billing_invoice_month,
    ROUND(SUM(cost), 3) AS monthly_cost,
    ROUND(SUM(net_cost), 3) AS monthly_net_cost,
  FROM (
    SELECT
      *,
      COALESCE((
        SELECT
          SUM(x.amount)
        FROM
          UNNEST(billing.credits) x),
        0) + cost AS net_cost
    FROM
      `$BILLING_PROJECT_ID.$BILLING_DATASET.$BILLING_TABLE` billing
    WHERE
      currency = "$CURRENCY")
  GROUP BY
    billing_account_id,
    service.id,
    service.description,
    project.id,
    project.number,
    project.name,
    location_location,
    location.country,
    location_region,
    location_zone,
    billing_invoice_month) billing
INNER JOIN (
  SELECT
    billing_account_id,
    usage_month,
    service.id AS service_id,
    project.number AS project_number,
    location.location AS location_location,
    ROUND(carbon_footprint_kgCO2e, 3) AS carbon_footprint_kgCO2e,
  FROM
    `$CARBON_PROJECT_ID.$CARBON_DATASET.$CARBON_TABLE` ) carbon
ON
  carbon.project_number = billing.project_number
  AND carbon.billing_account_id = billing.billing_account_id
  AND carbon.usage_month = billing.billing_invoice_month
  AND carbon.location_location = billing.location_location
  AND carbon.service_id = billing.service_id
LEFT JOIN (
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
      UNNEST(project.labels) labels)
  GROUP BY
    1) label_asset
ON
  billing.project_number = label_asset.number
LEFT JOIN (
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
      currency = "$CURRENCY"
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
    billing_invoice_month ) billing_usage
ON
  billing_usage.project_number = billing.project_number
  AND billing_usage.billing_account_id = billing.billing_account_id
  AND billing_usage.billing_invoice_month = billing.billing_invoice_month
  AND billing_usage.location_location = billing.location_location
  AND billing_usage.service_id = billing.service_id