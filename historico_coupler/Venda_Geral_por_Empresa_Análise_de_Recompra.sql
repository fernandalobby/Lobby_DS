WITH variacoes AS (
  SELECT
    s.id AS sale_id,
    CASE 
      WHEN SUM(si.amount * si.final_price) <= 0 THEN 1
      ELSE s.total_price / SUM(si.amount * si.final_price)
    END AS variacao
  FROM sales s
  JOIN sale_items si ON si.sale_id = s.id
  WHERE s.status <> 'CANCELED'
    AND si.status <> 'CANCELED'
  GROUP BY s.id, s.total_price
),

base_venda AS (
  SELECT
    s.id AS codigo_venda,
    s.company_id,
    c.name AS cliente,
    p.sku,
    (s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS data_venda,
    si.amount AS quantidade,
    ROUND((si.amount * si.final_price) * v.variacao, 2) AS valor_total,
    COALESCE(pl.name, 'Sem Plano') AS plano
  FROM sales s
  JOIN sale_items si ON si.sale_id = s.id
  JOIN products p ON p.id = si.product_id
  JOIN companies c ON c.id = s.company_id
  LEFT JOIN variacoes v ON v.sale_id = s.id
  LEFT JOIN subscriptions sub ON sub.company_id = c.id
  LEFT JOIN plans pl ON pl.id = sub.plan_id
  WHERE s.status <> 'CANCELED'
    AND si.status <> 'CANCELED'
    AND p.sku IS NOT NULL
)

SELECT
  cliente,
  sku,
  data_venda,
  codigo_venda,
  quantidade,
  valor_total,
  plano
FROM base_venda
ORDER BY cliente, sku, data_venda;
