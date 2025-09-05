SELECT 
    c.name AS "Empresa",
    cp.sku AS "SKU",
    TO_CHAR(sh.shipped_at AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data do Envio",
    TO_CHAR(so.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD-MM-YYYY') AS "Data da Solicitação de Envio",
    STRING_AGG(DISTINCT COALESCE(p.name, cp.name), ', ') AS "Nome Produtos",
    SUM(si.quantity) AS "Quantidade Total",
    STRING_AGG(DISTINCT ss.company_name::text, ', ') AS "Transportadoras",
    STRING_AGG(DISTINCT ss.name::text, ', ') AS "Métodos de Envio",
    STRING_AGG(DISTINCT so.code::text, ', ') AS "Pedidos de Envio",
    sh.status AS "Status do Envio",
    s.code as "Código da Venda"
FROM 
    shipments sh
LEFT JOIN shipping_services ss ON sh.shipping_service_id = ss.id 
LEFT JOIN shipping_orders so ON sh.shipping_order_id = so.id
LEFT JOIN companies c ON so.company_id = c.id
LEFT JOIN shipment_items si ON sh.id = si.shipment_id
LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
LEFT JOIN products p ON cp.product_id = p.id
left join shipping_order_sales sos on so.id = sos.shipping_order_id
left join sales s on s.id = sos.sale_id
WHERE 
    so.status <> 'canceled'
    AND sh.deleted_at IS NULL
    AND so.deleted_at IS NULL
    --AND sh.shipped_at >= CURRENT_DATE - INTERVAL '90 days'
   AND sh.shipped_at >=  '2025-01-01'
    AND sh.shipment_category IN ('SHIPPING', 'DEVOLUTION_RESEND')
GROUP BY 
    c.name, cp.sku, sh.shipped_at, so.created_at, sh.status, s.code
ORDER BY 
    c.name, cp.sku, sh.shipped_at;
