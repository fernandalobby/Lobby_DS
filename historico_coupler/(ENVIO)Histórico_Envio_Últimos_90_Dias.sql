WITH seguidores AS (
    SELECT
        sof.shipping_order_id,
        STRING_AGG(DISTINCT CONCAT(u.first_name, ' ', u.last_name), ', ') AS solicitantes
    FROM shipping_orders_followers sof
    LEFT JOIN users u ON sof.follower_id = u.id
    GROUP BY sof.shipping_order_id
),
entrega_cliente AS (
    SELECT 
        item_id::uuid AS shipment_id,
        MIN(created_at) AS data_entrega_cliente
    FROM versions
    WHERE event = 'update'
      AND item_type = 'Shipment'
      AND object_changes::jsonb -> 'status' ->> 1 = 'DELIVERED'
    GROUP BY item_id
),
itens_envio AS (
    SELECT 
        si.shipment_id,
        SUM(si.quantity) AS produto_quantidade
    FROM shipment_items si
    GROUP BY si.shipment_id
)
SELECT 
    -- Identificação do Pedido
    so.code AS "Pedido de Envio",
    c.name AS "Cliente",
    seg.solicitantes AS "Solicitante",
    t.name AS "Time",
    STRING_AGG(DISTINCT so.title, ', ') AS "Nome do Pedido de Envio",

    -- Destinatário
    sh.recipient_name AS "Destinatário", 

    sh.tracking_code AS "Código de Rastreio", 
    STRING_AGG(DISTINCT ck.name, ', ') AS "Kit do Cliente",
    so.shipping_purpose AS "Objetivo do Envio",

    -- Produtos
    STRING_AGG(DISTINCT cp.sku, ', ') AS "Skus",
    STRING_AGG(DISTINCT COALESCE(p.name, cp.name), ', ') AS "Nome Produtos",
    STRING_AGG(DISTINCT sh.shirt_size, ', ') AS "Tamanhos",

    -- Preço e Frete
    CASE 
        WHEN SUM(cp.unit_price) IS NULL OR SUM(cp.unit_price) = 0 
        THEN sh.declared_value 
        ELSE SUM(cp.unit_price) 
    END AS "Preço",
sh.shipment_price AS "Frete",
    -- Localização
    sh.recipient_city AS "Cidade", 
    sh.recipient_state AS "Estado",

    -- Datas
    TO_CHAR(so.created_at AT TIME zone 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data da Solicitação",
    TO_CHAR(so.shipment_expected_to AT TIME zone 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data do Envio",
    TO_CHAR(so.shipment_expected_to AT TIME zone 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'MM') AS "Mês de Envio",
    TO_CHAR(so.shipment_expected_to AT TIME zone 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'YYYY') AS "Ano de Envio",

    -- Status e Canal
    sh.status AS "Status", 
    so.created_through AS "Meio de Solicitação",

    -- Quantidades
    MAX(ie.produto_quantidade) AS "Quantidade Total",
    --SUM(CASE WHEN so.assembly_type != 'loose_items' THEN si.quantity ELSE 0 END) AS "Quantidade Manuseada",

    -- Endereço Completo
    --sh.recipient_zipcode AS "CEP", 
    --sh.recipient_street AS "Logradouro",
    --sh.recipient_number AS "Número",
    --sh.recipient_complement AS "Complemento",
    --sh.recipient_neighborhood AS "Bairro",

    -- Transporte
    sh.shipment_category AS "Categoria de Envio", 
    ss.company_name AS "Transportadora", 
    ss.name AS "Método de Envio", 
    CASE 
        WHEN sh.shipped_at IS NOT NULL AND ec.data_entrega_cliente IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (ec.data_entrega_cliente - sh.shipped_at)) / 86400)::int
        ELSE NULL
    END AS "Tempo de Entrega em Dias"

    -- Identificação do Frete
   --sh.service_bill_id AS "service_bill_id"

FROM 
    shipments sh
LEFT JOIN 
    shipping_services ss ON sh.shipping_service_id = ss.id 
LEFT JOIN 
    shipping_orders so ON sh.shipping_order_id = so.id
LEFT JOIN 
    companies c ON so.company_id = c.id
LEFT JOIN 
    shipping_orders_customer_kits sock ON so.id = sock.shipping_order_id
LEFT JOIN 
    customer_kits ck ON ck.id = sock.customer_kit_id
LEFT JOIN 
    seguidores seg ON so.id = seg.shipping_order_id
LEFT JOIN 
    teams t ON t.id = so.team_id
LEFT JOIN 
    shipment_items si ON sh.id = si.shipment_id
LEFT JOIN 
    customer_products cp ON si.customer_product_id = cp.id
LEFT JOIN 
    products p ON cp.product_id = p.id
LEFT JOIN 
    sizes_grids sg ON cp.sizes_grid_id = sg.id
LEFT JOIN 
	entrega_cliente ec ON ec.shipment_id = sh.id
LEFT JOIN itens_envio ie ON sh.id = ie.shipment_id


WHERE 
    so.status NOT IN ('CANCELED', 'canceled')
    AND sh.deleted_at IS NULL
    AND so.deleted_at IS NULL
    AND so.shipment_expected_to >= CURRENT_DATE - INTERVAL '96 days'
    AND sh.shipment_category IN ('SHIPPING','DEVOLUTION_RESEND')
    --AND sh.service_bill_id IS NOT NULL
    --AND so.code = '19149'

GROUP BY 
    so.code, so.shipment_expected_to, sh.status, c.name, so.created_at, so.title, 
    sh.recipient_name, sh.recipient_zipcode, sh.recipient_state,
    sh.shipment_category, ss.company_name, ss.name, sh.shipment_price, sh.recipient_city, 
    so.shipping_purpose, so.created_through, t.name, sh.tracking_code, seg.solicitantes,
    sh.declared_value, sh.recipient_street, sh.recipient_number, sh.recipient_complement,
    sh.recipient_neighborhood, sh.service_bill_id,sh.recipient_document_number,sh.shipped_at,
    ec.data_entrega_cliente

ORDER BY 
    so.code ASC;  
