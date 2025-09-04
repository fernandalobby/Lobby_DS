WITH itens_envio AS (
    SELECT 
        sh.shipping_order_id,
        SUM(si.quantity) AS soma_quantidade_peças
    FROM shipment_items si
    LEFT JOIN shipments sh ON si.shipment_id = sh.id
    WHERE sh.deleted_at IS NULL
    GROUP BY sh.shipping_order_id
)
SELECT
    so.code,
    TO_CHAR(so.created_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS created_at,
    TO_CHAR(sh.shipped_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS shipped_at,
    so.total_kits_quantity,
    ie.soma_quantidade_peças,
    
    EXTRACT(MONTH FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS mês_created_at,
    EXTRACT(YEAR FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS ano_created_at,
    
    CASE 
        WHEN sh.shipped_at IS NOT NULL 
        THEN EXTRACT(MONTH FROM sh.shipped_at AT TIME ZONE 'America/Sao_Paulo') 
        ELSE NULL 
    END AS mês_shipped_at,
    
    CASE 
        WHEN sh.shipped_at IS NOT NULL 
        THEN EXTRACT(YEAR FROM sh.shipped_at AT TIME ZONE 'America/Sao_Paulo') 
        ELSE NULL 
    END AS ano_shipped_at,
    
    EXTRACT(WEEK FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') AS num_semana_created_at

FROM shipping_orders so
LEFT JOIN shipments sh ON sh.shipping_order_id = so.id
LEFT JOIN shipping_services ss ON sh.shipping_service_id = ss.id
LEFT JOIN itens_envio ie ON so.id = ie.shipping_order_id

WHERE  
    so.status NOT IN ('canceled', 'draft')
    AND EXTRACT(YEAR FROM so.created_at AT TIME ZONE 'America/Sao_Paulo') IN (2024, 2025)
    AND sh.deleted_at IS NULL

GROUP BY  
    so.code,
    so.created_at,
    sh.shipped_at,
    so.total_kits_quantity,
    ie.soma_quantidade_peças
