SELECT 
    -- Informações do Cliente
    c.name AS "Cliente",
    c.financial_email AS "E-mail do Cliente",

    -- Pedido de Envio
    so.code AS "Número do Pedido de Envio",
    so.title AS "Nome do Pedido de Envio",

    -- Time e Solicitante
    STRING_AGG(DISTINCT CONCAT(u.first_name, ' ', u.last_name), ', ') AS "Solicitante",
    STRING_AGG(DISTINCT u.email, ', ') AS "E-mail do Solicitante",  

    -- Destinatário e Informações de Envio
    sh.recipient_name AS "Destinatário", 
    sh.recipient_document_number AS "CPF/CNPJ Destinatário",
    sh.recipient_zipcode AS "CEP", 
    sh.recipient_street AS "Logradouro", 
    sh.recipient_number AS "Número",  
    sh.recipient_complement AS "Complemento", 
    sh.recipient_neighborhood AS "Bairro", 
    sh.recipient_city AS "Cidade", 
    sh.recipient_state AS "Estado", 
    sh.recipient_email AS "E-mail do Destinatário", 

    -- Produtos e Itens do Pedido
    STRING_AGG(DISTINCT cp.sku, ', ') AS "SKUs",
    STRING_AGG(DISTINCT p.name, ', ') AS "Nome dos Produtos",
    STRING_AGG(DISTINCT sh.shirt_size, ', ') AS "Tamanhos de Camiseta",

    -- Rastreamento e Kit
    STRING_AGG(DISTINCT ck.name, ', ') AS "Kit do Cliente",

    -- Quantidade de Itens
    SUM(si.quantity) AS "Quantidade Total de Itens",

    -- Custos
    SUM(cp.unit_price * si.quantity) AS "Custo dos Produtos", 
    sh.shipment_price AS "Frete",

    -- Datas e Status
    TO_CHAR(so.shipment_expected_to AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data do Envio",
    sh.status AS "Status",

    -- Categoria de Envio e Transporte
    sh.shipment_category AS "Categoria de Envio", 
    ss.company_name AS "Transportadora", 
    ss.name AS "Método de Envio", 
    MAX(sh.days_to_deliver) AS "Dias para Entrega",

    -- Motivo da Devolução
    motivo_devolucao.descricoes AS "Motivo da Devolução",
    STRING_AGG(DISTINCT sh.tracking_code, ', ') AS "Código de Rastreio"

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
    shipping_orders_followers sof ON so.id = sof.shipping_order_id
LEFT JOIN 
    users u ON sof.follower_id = u.id
LEFT JOIN 
    shipment_items si ON sh.id = si.shipment_id
LEFT JOIN 
    customer_products cp ON si.customer_product_id = cp.id
LEFT JOIN 
    products p ON cp.product_id = p.id
LEFT JOIN 
    sizes_grids sg ON cp.sizes_grid_id = sg.id

-- Subquery lateral para extrair o motivo de devolução
LEFT JOIN LATERAL (
    SELECT 
        string_agg(DISTINCT evento->>'description', ', ') AS descricoes
    FROM 
        jsonb_array_elements(sh.tracking_events::jsonb) AS evento
    WHERE 
        evento->>'description' ILIKE '%Objeto não entregue%'
) motivo_devolucao ON TRUE

WHERE 
    so.status <> 'CANCELED' 
    AND sh.deleted_at IS NULL
    AND sh.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' >= (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '1 day'
    AND sh.status IN ('PACKAGE_RETURNED', 'DEVIATION')

GROUP BY 
    c.name, c.financial_email, so.code, so.title, sh.recipient_name, sh.recipient_document_number, 
    sh.recipient_zipcode, sh.recipient_street, sh.recipient_number, sh.recipient_complement, 
    sh.recipient_neighborhood, sh.recipient_city, sh.recipient_state, sh.recipient_email, 
    sh.shipment_category, ss.company_name, ss.name, sh.shipment_price, so.shipment_expected_to, 
    sh.status, sh.id, so.created_at, motivo_devolucao.descricoes

ORDER BY 
    so.code ASC;
