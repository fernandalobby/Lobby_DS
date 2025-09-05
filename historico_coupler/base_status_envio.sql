WITH quantidades_por_envio AS (
    SELECT 
        s.shipping_order_id, 
        SUM(COALESCE(si.quantity, 0)) AS quantity,
        SUM(COALESCE(cp.unit_price, 0) * COALESCE(si.quantity, 0)) AS valor_envio
    FROM shipment_items si
    LEFT JOIN shipments s ON s.id = si.shipment_id
    LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
    WHERE s.deleted_at IS NULL
    GROUP BY s.shipping_order_id
),
quantidades_por_pedido AS (
    SELECT 
        soi.shipping_order_id, 
        SUM(COALESCE(soi.quantity, 0) * COALESCE(so.total_kits_quantity, 1)) AS quantity,
        SUM(COALESCE(cp.unit_price, 0) * COALESCE(soi.quantity, 0) * COALESCE(so.total_kits_quantity, 1)) AS valor_envio
    FROM shipping_order_items soi
    LEFT JOIN shipping_orders so ON soi.shipping_order_id = so.id
    LEFT JOIN customer_products cp ON soi.customer_product_id = cp.id
    GROUP BY soi.shipping_order_id
),
skus_por_envio AS (
    SELECT 
        s.shipping_order_id,
        COUNT(DISTINCT cp.sku) AS total_skus
    FROM shipment_items si
    LEFT JOIN shipments s ON s.id = si.shipment_id
    LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
    WHERE s.deleted_at IS NULL
    GROUP BY s.shipping_order_id
),
skus_por_pedido AS (
    SELECT 
        soi.shipping_order_id,
        COUNT(DISTINCT cp.sku) AS total_skus
    FROM shipping_order_items soi
    LEFT JOIN customer_products cp ON soi.customer_product_id = cp.id
    GROUP BY soi.shipping_order_id
)

SELECT 
    so.code,
    TO_CHAR(so.created_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS created_at,
    TO_CHAR(so.shipment_expected_to AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS "data_esperada_de_saída",

    CASE 
        WHEN so.status = 'error' THEN '01. Erro'
        WHEN so.status = 'waiting_items_arrival' THEN '02. Aguardando chegada de itens'
        WHEN so.status = 'waiting_addresses' THEN '03. Aguardando endereços'
        WHEN so.status = 'waiting_logistics' THEN '04. Aguardando logística'
        WHEN so.status = 'waiting_picking' THEN '05. Aguardando picking'
        WHEN so.status = 'waiting_mounting' THEN '06. Aguardando montagem'
        WHEN so.status = 'mounting' THEN '07. Em montagem'
        WHEN so.status = 'waiting_for_printing' THEN '08. Aguardando impressão'
        WHEN so.status = 'waiting_labels' THEN '09. Aguardando etiquetas'
        WHEN so.status IN ('ready_to_ship', 'ready_for_pickup') THEN '10. Pronto para envio'
        ELSE so.status
    END AS status,

    c.name AS company_name, 
    so.total_kits_quantity AS montagem,

    CASE
        WHEN so.assembly_type = 'loose_items' THEN 'itens soltos'
        WHEN so.assembly_type = 'assembled_and_unsealed' THEN 'montado e sem lacre'
        WHEN so.assembly_type = 'assembled_and_sealed' THEN 'montado e lacrado'
        ELSE 'desconhecido'
    END AS pedidos,

    COALESCE(s_envio.total_skus, s_pedido.total_skus, 0) AS total_skus,
    COALESCE(q_envio.valor_envio, q_pedido.valor_envio, 0) AS total_valor_envio,
    COALESCE(q_envio.quantity, q_pedido.quantity, 0) AS total_quantity,

    CASE 
        WHEN so.shipment_expected_to < NOW() - INTERVAL '30 days' THEN 'Erro'
        WHEN so.shipment_expected_to BETWEEN NOW() - INTERVAL '30 days' AND NOW() - INTERVAL '1 day' THEN 'BackLog'
        ELSE 'Demanda Futura'
    END AS tempo_de_processo

FROM 
    shipping_orders so
LEFT JOIN quantidades_por_envio q_envio ON q_envio.shipping_order_id = so.id
LEFT JOIN quantidades_por_pedido q_pedido ON q_pedido.shipping_order_id = so.id
LEFT JOIN skus_por_envio s_envio ON s_envio.shipping_order_id = so.id
LEFT JOIN skus_por_pedido s_pedido ON s_pedido.shipping_order_id = so.id

LEFT JOIN companies c ON so.company_id = c.id

-- opcional: usuários, seguidores etc.
LEFT JOIN shipping_orders_followers sof ON so.id = sof.shipping_order_id
LEFT JOIN users u ON sof.follower_id = u.id

WHERE 
    so.status IN ('error', 'waiting_items_arrival', 'waiting_addresses', 'waiting_logistics', 
                  'waiting_picking', 'waiting_mounting', 'mounting', 'waiting_for_printing', 
                  'waiting_labels', 'ready_to_ship', 'ready_for_pickup')
    and so.shipment_expected_to IS NOT NULL
    --AND so.code IN ('19082', '19089') 

GROUP BY 
    so.code, so.shipment_expected_to, so.status, c.name, so.created_at, 
    so.total_kits_quantity, so.assembly_type, 
    q_envio.quantity, q_envio.valor_envio, 
    q_pedido.quantity, q_pedido.valor_envio,
    s_envio.total_skus, s_pedido.total_skus

ORDER BY 
    CASE 
        WHEN so.status = 'error' THEN 1
        WHEN so.status = 'waiting_items_arrival' THEN 2
        WHEN so.status = 'waiting_addresses' THEN 3
        WHEN so.status = 'waiting_logistics' THEN 4
        WHEN so.status = 'waiting_picking' THEN 5
        WHEN so.status = 'waiting_mounting' THEN 6
        WHEN so.status = 'mounting' THEN 7
        WHEN so.status = 'waiting_for_printing' THEN 8
        WHEN so.status = 'waiting_labels' THEN 9
        WHEN so.status IN ('ready_to_ship', 'ready_for_pickup') THEN 10
        ELSE 11
    END,
    so.shipment_expected_to ASC,
    so.code ASC;
