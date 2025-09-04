SELECT
  CAST(q.created_at AS date) AS data,
  q.code AS numero_orcamento,
  s.code AS numero_venda,
  p.sku AS sku,
  p.name AS sku_nome,
  pc.name AS category,
  q.kit_purpose AS kit_purpouse,
  au_seller.first_name AS seller,
  CASE
    WHEN q.is_event = 'TRUE' AND q.turbo = 'TRUE' THEN 'TURBO+EVENTO'
    WHEN q.is_event = 'TRUE' AND q.turbo <> 'TRUE' THEN 'EVENTO'
    WHEN q.is_event <> 'TRUE' AND q.turbo = 'TRUE' THEN 'TURBO'
    ELSE 'PADRAO'
  END AS "TURBOouEVENTO",
  qi.amount AS qtdade,
  CAST(q.total_price AS float8) AS total_orcamento,
  CAST(qi.amount * qi.final_price AS numeric) AS valor_orcamento,
  s.status AS status_venda,
  sup_supplier.trading_name AS default_supplier_name,
  qr.company_name AS company,
  qr.job_role
FROM quotes q
LEFT JOIN quote_items qi ON q.id = qi.quote_id
LEFT JOIN products p ON qi.product_id = p.id
LEFT JOIN product_categories pc ON p.category_id = pc.id
LEFT JOIN sales s ON s.quote_id = q.id
LEFT JOIN admin_users au_seller ON q.seller_id::bigint = au_seller.id
LEFT JOIN suppliers sup_supplier ON p.default_supplier_id = sup_supplier.id
LEFT JOIN quote_requesters qr ON q.id = qr.quote_id
WHERE 
  q.created_at >= '2024-01-01'::date
  AND (s.status IS NULL OR s.status NOT IN ('CANCELED'));
