SELECT 
    -- Período
    so.code AS "Pedido de Envio",
    c.name AS "Cliente",
     (select
        STRING_AGG(DISTINCT CONCAT(u.first_name, ' ', u.last_name), ', ')
        from shipping_orders_followers sof
        join users u on u.id = sof.follower_id
        where sof.shipping_order_id = so.id
    ) AS "Solicitante",
    t.name AS "Time",  -- Agora relacionando diretamente ao pedido de envio
    STRING_AGG(DISTINCT so.title, ', ') AS "Nome do Pedido de Envio",
    sh.recipient_name AS "Destinatário", 
    sh.tracking_code AS "Código de Rastreio", 
    so.shipping_purpose as "Objetivo do Envio",
    STRING_AGG(DISTINCT cp.sku, ', ') AS "Skus",
    STRING_AGG(DISTINCT COALESCE(p.name, cp.name), ', ') AS "Nome Produtos",
    STRING_AGG(DISTINCT sh.shirt_size, ', ') AS "Tamanhos",
    SUM(cp.unit_price) AS "Preço", 
    sh.shipment_price AS "Frete",
    sh.recipient_city AS "Cidade", 
    sh.recipient_state AS "Estado", 
    TO_CHAR(so.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data da Solicitação",
    TO_CHAR(so.shipment_expected_to AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data do Envio",
    sh.status AS "Status", 
    so.created_through as "Meio de Solicitação",
    SUM(si.quantity) AS "Quantidade Total",
    SUM(CASE WHEN so.assembly_type != 'loose_items' THEN si.quantity ELSE 0 END) AS "Quantidade Manuseada",
    sh.recipient_zipcode AS "Endereço", 
    sh.shipment_category AS "Categoria de Envio", 
    ss.company_name AS "Transportadora", 
    ss.name AS "Método de Envio", 
    MAX(sh.days_to_deliver) AS "Dias para Entrega",
    sh.service_bill_id
FROM 
    shipments sh
LEFT JOIN 
    shipping_services ss ON sh.shipping_service_id = ss.id 
LEFT JOIN 
    shipping_orders so ON sh.shipping_order_id = so.id
LEFT JOIN 
    companies c ON so.company_id = c.id
LEFT JOIN 
    teams t ON t.id = so.team_id  -- Agora relacionando o time diretamente ao pedido de envio
LEFT JOIN 
    shipment_items si ON sh.id = si.shipment_id
LEFT JOIN 
    customer_products cp ON si.customer_product_id = cp.id
LEFT JOIN 
    products p ON cp.product_id = p.id
LEFT JOIN 
    sizes_grids sg ON cp.sizes_grid_id = sg.id
WHERE 
    so.status <> 'canceled'
    AND sh.deleted_at IS NULL
    AND so.deleted_at IS NULL
    AND so.shipment_expected_to >=  '2024-01-01'
    --and extract(month from so.shipment_expected_to) = '2'
    --and c.name = 'Alura'
    and sh.shipment_category in ('SHIPPING', 'DEVOLUTION_RESEND')
    --and sh.service_bill_id is not null
GROUP BY 
    so.id, so.code, so.shipment_expected_to, sh.status, c.name, so.created_at, so.title, 
    sh.recipient_name, sh.recipient_zipcode, sh.recipient_state,
    sh.shipment_category, ss.company_name, ss.name, sh.shipment_price, sh.recipient_city, 
    so.shipping_purpose, so.created_through, t.name,sh.tracking_code, sh.service_bill_id -- Adicionando t.name no GROUP by
ORDER BY 
    so.code ASC;
