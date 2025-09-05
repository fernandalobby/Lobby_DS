WITH unique_items AS (
  SELECT DISTINCT
    si.id AS sale_item_id,
    cp.internal_note,
    c.name AS cliente,
    cp.sku,
    COALESCE(p.name, cp.name) AS produto,
    pc.name AS categoria,
    si.amount,
    EXTRACT(MONTH FROM si.created_at) AS mes,
    EXTRACT(YEAR FROM si.created_at) AS ano
  FROM sale_items si
  LEFT JOIN inventory_moves im ON im.sale_item_id = si.id
  LEFT JOIN customer_products cp ON im.customer_product_id = cp.id 
  LEFT JOIN companies c ON cp.company_id = c.id
  LEFT JOIN products p ON cp.product_id = p.id
  LEFT JOIN product_categories pc ON p.category_id = pc.id
  WHERE si.created_at >= '2024-09-01'
)
SELECT 
  ui.internal_note AS "Código da Venda",
  ui.cliente AS "Cliente",
  ui.sku AS "SKU",
  ui.produto AS "Produto",
  ui.categoria AS "Categoria do Produto",
  COALESCE(ss.trading_name, 'Não informado') AS "Fornecedor do Produto",
  COALESCE(sc.trading_name, 'Não informado') AS "Fornecedor de Personalização",
  ui.amount AS "Quantidade de Itens Vendidos",
  ui.mes AS "Mês",
  ui.ano AS "Ano"
FROM unique_items ui
LEFT JOIN sale_item_production_infos sipi ON ui.sale_item_id = sipi.sale_item_id
LEFT JOIN suppliers ss ON sipi.product_supplier_id = ss.id
LEFT JOIN suppliers sc ON sipi.product_customizer_id = sc.id
ORDER BY ui.internal_note, ui.sku;
