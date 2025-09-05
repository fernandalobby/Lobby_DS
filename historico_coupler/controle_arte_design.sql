WITH quote_item_images AS (
  SELECT DISTINCT
    v.object_changes::jsonb->'quote_item_id'->>1 AS quote_item_id,
    v.created_at,
    v.event,
    v.object_changes::jsonb->'image_data'->1->>'id' AS image_id,
    v.object_changes::jsonb->'imaage_data'->1->'metadata'->>'filename' AS filename
  FROM versions v
  WHERE v.item_type = 'QuoteItemImage'
    AND v.object_changes::jsonb->'quote_item_id'->>1 IS NOT NULL
),
quote_items_info AS (
  SELECT 
    id AS quote_item_id,
    quote_id,
    product_id,
    created_at AS quote_item_created_at
  FROM quote_items
),
images_joined AS (
  SELECT DISTINCT
    qi.image_id,
    qi.created_at,
    qi.event,
    qi.filename,
    qii.quote_id,
    qii.product_id,
    qii.quote_item_created_at
  FROM quote_item_images qi
  JOIN quote_items_info qii ON qi.quote_item_id::uuid = qii.quote_item_id
  WHERE qii.quote_id IS NOT NULL
),
quotes_and_products AS (
  SELECT DISTINCT
    qu.id AS quote_id,
    qu.code AS quote_code,
    qu.kit_purpose,
    qu.created_at AS quote_created_at,
    qu.designer_id,
    qit.product_id,
    qit.quote_item_created_at,
    COALESCE(p.sku, cp.sku) AS sku,
    COALESCE(p.name, cp.name) AS product_name,
    pc.name AS category_name,
    CASE WHEN s.id IS NOT NULL THEN 'Sim' ELSE 'Não' END AS virou_venda,
    (au.first_name || ' ' || au.last_name) AS designer_name
  FROM quotes qu
  JOIN quote_items_info qit ON qit.quote_id = qu.id
  LEFT JOIN products p ON qit.product_id = p.id
  LEFT JOIN customer_products cp ON qit.product_id = cp.product_id
  LEFT JOIN product_categories pc ON p.category_id = pc.id
  LEFT JOIN sales s ON s.quote_id = qu.id
  LEFT JOIN admin_users au ON qu.designer_id = au.id
  WHERE qu.created_at >= '2024-01-01'
)
SELECT
  qap.quote_code AS "Código Orçamento",
  EXTRACT(MONTH FROM qap.quote_created_at) as "Mês Criação Orçamento",
  EXTRACT(YEAR FROM qap.quote_created_at) as "Ano Criação Orçamento",
  qap.kit_purpose AS "Nome Orçamento",
  qap.sku AS "SKU",
  qap.product_name AS "Nome Produto",
  qap.category_name AS "Categoria Produto",
  qap.virou_venda AS "Virou Venda",
  COUNT(DISTINCT ij.image_id) FILTER (WHERE ij.event = 'create') AS "Qtd Imagens",
  MIN(ij.created_at) FILTER (WHERE ij.event = 'create') AS "Data Criação Primeira Imagem",
  MAX(ij.created_at) FILTER (WHERE ij.event = 'create') AS "Data Criação Última Imagem",
  STRING_AGG(DISTINCT ij.filename, ', ') FILTER (WHERE ij.filename IS NOT NULL) AS "Arquivos",
  qap.designer_name AS "Designer",
  EXTRACT(MONTH FROM MAX(ij.created_at) FILTER (WHERE ij.event = 'create')) as "Mês Criação Arte",
  EXTRACT(YEAR FROM MAX(ij.created_at) FILTER (WHERE ij.event = 'create')) as "Ano Criação Arte",
(
  CASE
    WHEN MIN(ij.created_at) FILTER (WHERE ij.event = 'create') < MIN(qap.quote_item_created_at) THEN NULL

    WHEN DATE_TRUNC('day', MIN(ij.created_at)) = DATE_TRUNC('day', MIN(qap.quote_item_created_at)) THEN
      GREATEST(EXTRACT(EPOCH FROM (
        MIN(ij.created_at) - MIN(qap.quote_item_created_at)
      )) / 3600, 0)

    WHEN MIN(qap.quote_item_created_at)::time > '18:00:00' AND DATE_TRUNC('day', MIN(ij.created_at)) = DATE_TRUNC('day', MIN(qap.quote_item_created_at) + INTERVAL '1 day') THEN
      EXTRACT(EPOCH FROM (
        MIN(ij.created_at) - (DATE_TRUNC('day', MIN(qap.quote_item_created_at)) + INTERVAL '1 day' + INTERVAL '9 hours')
      )) / 3600

    WHEN MIN(qap.quote_item_created_at)::time <= '18:00:00' AND DATE_TRUNC('day', MIN(ij.created_at)) = DATE_TRUNC('day', MIN(qap.quote_item_created_at) + INTERVAL '1 day') THEN
      (EXTRACT(EPOCH FROM (
        MIN(ij.created_at) - (DATE_TRUNC('day', MIN(qap.quote_item_created_at)) + INTERVAL '1 day' + INTERVAL '9 hours')
      )) / 3600) +
      (EXTRACT(EPOCH FROM (
        (DATE_TRUNC('day', MIN(qap.quote_item_created_at)) + INTERVAL '18 hours') - MIN(qap.quote_item_created_at)
      )) / 3600)

    WHEN DATE_TRUNC('day', MIN(ij.created_at)) > DATE_TRUNC('day', MIN(qap.quote_item_created_at) + INTERVAL '1 day') THEN
      (EXTRACT(EPOCH FROM (
        MIN(ij.created_at) - (DATE_TRUNC('day', MIN(ij.created_at)) + INTERVAL '9 hours')
      )) / 3600) +
      (EXTRACT(EPOCH FROM (
        (DATE_TRUNC('day', MIN(qap.quote_item_created_at)) + INTERVAL '18 hours') - MIN(qap.quote_item_created_at)
      )) / 3600) +
      (
        SELECT COUNT(*) 
        FROM generate_series(
          DATE_TRUNC('day', MIN(qap.quote_item_created_at)) + INTERVAL '1 day',
          DATE_TRUNC('day', MIN(ij.created_at)) - INTERVAL '1 day',
          INTERVAL '1 day'
        ) AS gs(day)
        WHERE EXTRACT(DOW FROM gs.day) NOT IN (0, 6)  -- Seg a Sex
          AND gs.day NOT IN (
            SELECT unnest(ARRAY[
              '2024-01-01', '2024-02-12', '2024-02-13', '2024-03-29', '2024-04-21', '2024-05-01',
              '2024-05-30', '2024-09-07', '2024-10-12', '2024-11-02', '2024-11-15', '2024-12-25',
              '2025-01-01', '2025-03-03', '2025-03-04', '2025-04-18', '2025-04-21', '2025-05-01',
              '2025-06-19', '2025-09-07', '2025-10-12', '2025-11-02', '2025-11-15', '2025-12-25'
            ])::date
          )
      ) * 9

    ELSE NULL
  END
) AS "Tempo até Primeira Arte"
FROM images_joined ij
JOIN quotes_and_products qap ON ij.quote_id = qap.quote_id AND ij.product_id = qap.product_id
GROUP BY qap.quote_code, qap.kit_purpose, qap.sku, qap.product_name, qap.category_name, qap.virou_venda, qap.quote_created_at, qap.designer_name, qap.quote_item_created_at
HAVING COUNT(DISTINCT ij.image_id) FILTER (WHERE ij.event = 'create') > 0
ORDER BY MAX(ij.created_at) DESC;
