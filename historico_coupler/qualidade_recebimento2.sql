SELECT 
  cp.internal_note AS "Código da Venda",
  c."name" AS "Cliente",
  cp.sku AS "SKU",
  COALESCE(p.name, cp."name") AS "Produto",

  CASE 
    WHEN lr.reason = 'kNEADED' THEN 'Amassado'
    WHEN lr.reason = 'RISKS' THEN 'Riscado'
    WHEN lr.reason = 'DEFECTIVE_ART' THEN 'Defeito na Arte'
    WHEN lr.reason = 'NO_ART' THEN 'Sem Arte'
    ELSE lr.reason
  END AS "Razão do Relatório de Perda",

  lr.quantity AS "Quantidade Relatada na Perda",
  lr.created_at AS "Criado em",

  ss.trading_name AS "Fornecedor do Produto",
  sc.trading_name AS "Fornecedor de Personalização",
  pc.name AS "Categoria do Produto",
 si.amount as "Qtdade Total",
 EXTRACT(MONTH FROM si.created_at) AS "Mês",
 EXTRACT(YEAR FROM si.created_at) AS "Ano"


FROM loss_reports lr
LEFT JOIN inventory_moves im ON im.id = lr.inventory_move_id
LEFT JOIN sale_items si ON im.sale_item_id = si.id 
LEFT JOIN sales s ON si.sale_id = s.id
LEFT JOIN sale_item_production_infos sipi ON si.id = sipi.sale_item_id
LEFT JOIN suppliers ss ON sipi.product_supplier_id = ss.id
LEFT JOIN suppliers sc ON sipi.product_customizer_id = sc.id
LEFT JOIN customer_products cp ON im.customer_product_id = cp.id 
LEFT JOIN companies c ON cp.company_id = c.id
LEFT JOIN products p ON cp.product_id = p.id
LEFT JOIN product_categories pc ON p.category_id = pc.id
