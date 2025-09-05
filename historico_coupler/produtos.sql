WITH versions_filtered AS (
  SELECT *
  FROM versions
  WHERE item_type IN ('Product', 'ProductImage', 'PricingRange')
),
product_filtered AS (
  SELECT id::TEXT AS product_id, sku
  FROM products
),
image_filtered AS (
  SELECT id::TEXT AS image_id, product_id::TEXT AS product_id
  FROM product_images
),
pricing_filtered AS (
  SELECT id::TEXT AS pricing_id, product_id::TEXT AS product_id
  FROM pricing_ranges
),
version_with_sku AS (
  SELECT
    (v.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS created_at,
    pf.sku
  FROM versions_filtered v
  LEFT JOIN product_filtered pf
    ON v.item_type = 'Product' AND v.item_id::TEXT = pf.product_id

  UNION ALL

  SELECT
    (v.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS created_at,
    p_img.sku
  FROM versions_filtered v
  JOIN image_filtered pi
    ON v.item_type = 'ProductImage' AND v.item_id::TEXT = pi.image_id
  JOIN product_filtered p_img
    ON p_img.product_id = pi.product_id
  UNION ALL
  SELECT
    (v.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS created_at,
    p_pr.sku
  FROM versions_filtered v
  JOIN pricing_filtered pr
    ON v.item_type = 'PricingRange' AND v.item_id::TEXT = pr.pricing_id
  JOIN product_filtered p_pr
    ON p_pr.product_id = pr.product_id
)
SELECT
  sku,
  TO_CHAR(MIN(created_at), 'DD/MM/YYYY') AS real_create_date
FROM version_with_sku
WHERE sku IS NOT NULL
GROUP BY sku
ORDER BY sku
