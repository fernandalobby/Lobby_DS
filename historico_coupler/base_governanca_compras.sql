SELECT  
  s.id,  
  s.code,  
  si.amount "quantidade_venda",  
  CAST((si.amount) * (si.product_price) AS FLOAT) "venda_original",  
  CAST((si.amount) * (si.final_price) AS FLOAT) "venda_final",  
  --dates--  
  to_char(s.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "created_at",  
  to_char(s.expected_delivery AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "expected_delivery_at",  
  to_char(s.event_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "event_at",  
  to_char(sipi.purchase_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "purchase_at",  
  to_char(sipi.customizer_arrival_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "customization_at",  
  to_char(sipi.arrival_forecast_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "expected_arrival_at",  
  to_char(im.arrived_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "arrival_at",  
  prod."TO_PRODUCE_AT",  
  --prod."BUY_AT",  
  s.status "status_pedido",  
  si.status "Status_produto",  
   case when s.is_event = 'TRUE'then'EVENTO'else ''END AS "EVENTO",  
 case when s.turbo = 'TRUE'then'TURBO'else ''END AS "TURBO",  
  --s.is_event "event",  
  --tags--  
  c.name "client",  
  p.name "product",  
  pc.name "category",  
  ss.trading_name AS supplier,  
  sc.trading_name AS customizer,  
  --margin--  
  CAST(sipi.purchased_quantity AS FLOAT) "client_quantity",  
  CAST(sipi.exceeding_quantity AS FLOAT),  
  CAST((sipi.purchased_quantity + sipi.exceeding_quantity) AS FLOAT) "production_quantity",  
  CAST(sipi.unit_cost AS FLOAT),  
  CAST(sipi.customize_cost AS FLOAT),  
  CAST((sipi.unit_cost + sipi.customize_cost) AS FLOAT) "product_cost",  
  '' as "discount",  
  CAST(si.product_price AS FLOAT),  
  CAST(si.base_price AS FLOAT),  
  CAST(si.final_price AS FLOAT) "price_with_addons",  
  --entry--  
  CAST(im.expected_quantity AS FLOAT),  
  CAST(im.quantity AS FLOAT) "received_quantity",  
  CAST(im.scrap_quantity AS FLOAT)"scrap_quantity",  
  ''  as exceeding_quantity2,  
  au_seller.first_name AS seller,  
  au.first_name AS Buyer,  
  sipi.filled as custo_preenchido,
  --  sidi.filled as "arte_disp",
     case when sidi.filled = 'TRUE'then'ARTE OK'else 'FALTA ARTE'END AS "arte_disp",  
    sidi.customization_type as "Tipo_personalizacao",
  au_designer.first_name AS designer ,
  --sipi.notes  
  s.storage,
  sipi.buyer_id,
  sipi.omie_order_code
FROM  
  sales s  
JOIN  
  companies c ON c.id = s.company_id  
LEFT JOIN  
  sale_items si ON s.id = si.sale_id  
LEFT JOIN  
  sale_item_production_infos sipi ON si.id = sipi.sale_item_id  
LEFT JOIN  
  sale_item_design_infos sidi ON si.id = sidi.sale_item_id  
LEFT JOIN  
  products p ON p.id = si.product_id  
LEFT JOIN  
  product_categories pc ON pc.id = p.category_id  
LEFT JOIN  
  inventory_moves im ON si.id = im.sale_item_id  
LEFT JOIN  
  suppliers ss ON sipi.product_supplier_id = ss.id  
LEFT JOIN  
  suppliers sc ON sipi.product_customizer_id = sc.id  
LEFT JOIN  
  admin_users au_seller ON s.seller_id::bigint = au_seller.id  
LEFT JOIN  
  admin_users au_designer ON s.designer_id::bigint = au_designer.id  
LEFT JOIN  
  admin_users au ON s.production_assignee_id = au.id  
LEFT JOIN  
(  
  SELECT  
    s.code AS numero_pedido,  
    to_char(sse.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') "TO_PRODUCE_AT"  
  FROM sale_items si  
  JOIN sales s  
    ON s.id = si.sale_id  
  JOIN sale_status_edits sse  
    ON sse.sale_id = s.id  
    AND sse.new_status = 'TO_PRODUCE'  
  GROUP BY to_char(sse.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY'), s.code  
) prod ON s.code = prod.numero_pedido  
WHERE  
---s.created_at >= '2024-01-01'::date  
--and s.code = '1348'
 --si.status not IN ('CANCELED')
si.status not in ('CANCELED','DELIVERED')
--AND s.status not in ('CANCELED')
AND s.status  in ('TO_PRODUCE','PRODUCING','TO_BILL')
--AND im.status not in ('CANCELED')
