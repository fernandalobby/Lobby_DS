SELECT
  s.id,
  s.code,
  si.amount AS quantidade_venda,
  CAST((si.amount) * (si.final_price) AS FLOAT) AS venda_final_item,
  CAST((si.amount) * (si.final_price) AS FLOAT) * dtp.variacao AS venda_final_item_variacao,
(s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS created_at,
  to_char(s.expected_delivery AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS expected_delivery_at,
  to_char(s.event_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS event_at,
  to_char(sipi.purchase_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS purchase_at,
  to_char(sipi.customizer_arrival_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS customization_at,
  to_char(sipi.arrival_forecast_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS expected_arrival_at,
  im.arrived_at AS arrival_at,
  prod."TO_PRODUCE_AT",
  ue.ultima_entrega,
  s.status AS status_pedido,
  si.status AS Status_produto,
  CASE WHEN s.is_event = 'TRUE' THEN 'EVENTO' ELSE '' END AS EVENTO,
  CASE WHEN s.turbo = 'TRUE' THEN 'TURBO' ELSE '' END AS TURBO,
  c.name AS client,
  p.sku,
  p.name AS product,
  pc.name AS category,
  ss.trading_name AS supplier,
  sc.trading_name AS customizer,
  CAST(sipi.purchased_quantity AS FLOAT) AS client_quantity,
  COALESCE(CAST(sipi.exceeding_quantity AS FLOAT), 0) AS exceeding_quantity,
  CAST((sipi.purchased_quantity + sipi.exceeding_quantity) AS FLOAT) AS production_quantity,
  CAST(sipi.unit_cost AS FLOAT) AS unit_cost,
  CAST(sipi.customize_cost AS FLOAT) AS customize_cost,
  CAST((sipi.unit_cost + sipi.customize_cost) AS FLOAT) AS product_cost,
  CASE 
      WHEN si.price_modifier_type = 'PERCENTAGE' THEN CAST(si.price_modifier_value / 100 AS FLOAT)  
      WHEN si.price_modifier_type = 'VALUE' THEN CAST(si.price_modifier_value AS FLOAT)  
      ELSE 0 
  END AS discount,
  CAST(si.product_price AS FLOAT) AS product_price,
  CAST(si.base_price AS FLOAT) AS base_price,
  CAST(si.final_price AS FLOAT) AS price_with_addons,
  CAST(im.expected_quantity AS FLOAT) AS expected_quantity,
  CAST(im.quantity AS FLOAT) AS received_quantity,
  CAST(im.scrap_quantity AS FLOAT) AS scrap_quantity,
  COALESCE(CAST(sipi.exceeding_quantity AS FLOAT), 0) AS exceeding_quantity_2,
  au_seller.first_name AS seller,
  au.first_name AS buyer,
  '' AS cadastro_turbo,
  CASE WHEN sipi.filled = 'TRUE' THEN 'CUSTO OK' ELSE 'FALTA CUSTO' END AS custo_disp,
  CASE WHEN sidi.filled = 'TRUE' THEN 'ARTE OK' ELSE 'FALTA ARTE' END AS arte_disp,
  sidi.customization_type AS Tipo_personalizacao,
  au_designer.first_name AS designer,
  un.units,
  un.personalization_cost,
  CAST(un.product_cost AS FLOAT) AS product_cost_2,
  un.markup,
  si.personalization_areas_amount,
  si.personalization_colors_amount,
  sidi.customization_colors,
  sidi.art_size,
  s.storage,
  sipi.buyer_id,
  sipi.omie_order_code,
  CAST(p.created_at AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS product_created_at
FROM sales s
LEFT JOIN shipping_orders so ON s.id = so.id
JOIN companies c ON c.id = s.company_id
LEFT JOIN sale_items si ON s.id = si.sale_id
LEFT JOIN sale_item_production_infos sipi ON si.id = sipi.sale_item_id
LEFT JOIN sale_item_design_infos sidi ON si.id = sidi.sale_item_id
LEFT JOIN products p ON p.id = si.product_id
LEFT JOIN product_categories pc ON pc.id = p.category_id
LEFT JOIN (
  SELECT
    max(created_at), sale_item_id, arrived_at, expected_quantity, quantity, scrap_quantity, exceeding_quantity
  FROM inventory_moves
  WHERE quantity IS NOT NULL
    AND concat(created_at, sale_item_id) IN (
      SELECT concat(res.created_at, res.sale_item_id)
      FROM (
        SELECT max(created_at) AS created_at, sale_item_id
        FROM inventory_moves
        GROUP BY sale_item_id
      ) res
    )
  GROUP BY sale_item_id, arrived_at, expected_quantity, quantity, scrap_quantity, exceeding_quantity
) im ON si.id = im.sale_item_id
LEFT JOIN suppliers ss ON sipi.product_supplier_id = ss.id
LEFT JOIN suppliers sc ON sipi.product_customizer_id = sc.id
LEFT JOIN admin_users au_seller ON s.seller_id::bigint = au_seller.id
LEFT JOIN admin_users au_designer ON s.designer_id::bigint = au_designer.id
LEFT JOIN admin_users au ON s.production_assignee_id = au.id
LEFT JOIN (
  SELECT
    s.code AS numero_pedido,
    sse.sale_id,
    max(to_char(sse.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY')) AS "TO_PRODUCE_AT"
  FROM sale_items si
  JOIN sales s ON s.id = si.sale_id
  JOIN (
    SELECT max(created_at) AS created_at, sale_id, new_status
    FROM sale_status_edits
    GROUP BY sale_id, new_status
  ) sse ON sse.sale_id = s.id AND sse.new_status = 'TO_PRODUCE'
  GROUP BY to_char(sse.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY'), s.code, sse.sale_id
) prod ON s.code = prod.numero_pedido
LEFT JOIN (
  SELECT
    fq.code,
    pr.units,
    fq.sku,
    fq.created_at,
    personalization_cost,
    product_cost,
    markup,
    fq.quantidade_venda
  FROM (
    SELECT
      s.code,
      p.sku,
      si.created_at,
      si.amount AS quantidade_venda,
      p.id,
      CAST(
        CASE
          WHEN si.amount < 51 THEN 50
          WHEN si.amount < 101 THEN 100
          WHEN si.amount < 251 THEN 250
          WHEN si.amount < 501 THEN 500
          ELSE 1000
        END AS INTEGER
      ) AS faixa
    FROM sales s
    LEFT JOIN sale_items si ON s.id = si.sale_id
    LEFT JOIN products p ON p.id = si.product_id
    WHERE si.status NOT IN ('CANCELED')
  ) fq
  LEFT JOIN pricing_ranges pr ON pr.units = fq.faixa AND fq.id = pr.product_id
) un ON un.sku = p.sku AND un.code = s.code AND un.created_at = si.created_at
LEFT JOIN (
  SELECT
    ue.code,
    max(arrived_at) AS ultima_entrega
  FROM (
    SELECT s.code, im.arrived_at
    FROM sales s
    LEFT JOIN shipping_orders so ON s.id = so.id
    LEFT JOIN sale_items si ON s.id = si.sale_id
    LEFT JOIN inventory_moves im ON si.id = im.sale_item_id
    WHERE s.created_at >= '2024-01-01'::date
      AND s.status NOT IN ('CANCELED')
      AND si.status NOT IN ('CANCELED')
  ) ue
  GROUP BY ue.code
) ue ON s.code = ue.code
LEFT JOIN (
  SELECT
    s.code,
    s.total_price AS venda_total_final,
    CAST(SUM((si.amount) * (si.final_price)) AS FLOAT) AS VENDA,
    CASE
      WHEN SUM((si.amount) * (si.final_price)) <= 0 THEN 1
      ELSE CAST(SUM((si.amount) * (si.final_price)) AS FLOAT)
    END AS variacao2,
    CASE
      WHEN SUM((si.amount) * (si.final_price)) <= 0 THEN 1
      ELSE s.total_price
    END / CASE
      WHEN SUM((si.amount) * (si.final_price)) <= 0 THEN 1
      ELSE CAST(SUM((si.amount) * (si.final_price)) AS FLOAT)
    END AS variacao
  FROM sales s
  LEFT JOIN sale_items si ON s.id = si.sale_id
  WHERE
    s.status NOT IN ('CANCELED')
    AND si.status NOT IN ('CANCELED')
  GROUP BY s.total_price, s.code
) dtp ON s.code = dtp.code
WHERE
  s.created_at >= '2025-01-01'::date
  AND s.status <> ('CANCELED')
 AND si.status <> ('CANCELED')
