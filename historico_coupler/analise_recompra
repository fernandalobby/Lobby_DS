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
    s.id AS sales_id,
    s.code AS codigo_venda,
    s.company_id,
    p.sku,
    p.name AS nome_produto,
    pc.name AS categoria,
    (s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS data_venda,
    si.amount AS quantidade,
    ROUND((si.amount * si.final_price) * v.variacao, 2) AS valor_gasto,
    COALESCE(pl.name, 'Sem Plano') AS plano
  FROM sales s
  JOIN sale_items si ON si.sale_id = s.id
  JOIN products p ON p.id = si.product_id
  LEFT JOIN product_categories pc ON pc.id = p.category_id
  LEFT JOIN variacoes v ON v.sale_id = s.id
  LEFT JOIN companies c ON c.id = s.company_id
  LEFT JOIN subscriptions sub ON sub.company_id = c.id
  LEFT JOIN plans pl ON pl.id = sub.plan_id
  WHERE s.status <> 'CANCELED'
    AND si.status <> 'CANCELED'
    AND p.sku IS NOT NULL
),

recompras AS (
  SELECT
    a.company_id,
    a.sku,
    a.nome_produto,
    a.categoria AS categoria_primeira,
    b.categoria AS categoria_recompra,
    b.plano AS plano,
    a.data_venda AS data_primeira_compra,
    b.data_venda AS data_recompra,
    a.codigo_venda AS codigo_venda_original,
    b.codigo_venda AS codigo_venda_recompra,
    a.quantidade AS quantidade_primeira,
    b.quantidade AS quantidade_recompra,
    a.valor_gasto AS valor_primeira,
    b.valor_gasto AS valor_recompra,
   (b.data_venda - a.data_venda)::int AS dias_entre
  FROM base_venda a
  JOIN base_venda b
    ON a.company_id = b.company_id
   AND a.sku = b.sku
   AND a.data_venda < b.data_venda
   AND (b.data_venda - a.data_venda) >= 30
)

SELECT
  r.sku,
  r.nome_produto,
  c.name AS nome_empresa,
  r.codigo_venda_original,
  r.data_primeira_compra,
  r.quantidade_primeira,
  r.valor_primeira,
  r.codigo_venda_recompra,
  r.data_recompra,
  r.quantidade_recompra,
  r.valor_recompra,
  r.dias_entre,
  r.categoria_primeira,
  r.categoria_recompra,
  r.plano
FROM recompras r
JOIN companies c ON c.id = r.company_id
ORDER BY r.sku, nome_empresa, r.data_primeira_compra;
