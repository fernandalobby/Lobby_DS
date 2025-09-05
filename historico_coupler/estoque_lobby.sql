WITH team_per_company AS ( 
    SELECT DISTINCT ON (c.id) 
        c.id AS company_id, 
        t.name AS team_name
    FROM companies c
    LEFT JOIN teams t ON t.company_id = c.id
),
product_origins AS (
    SELECT 
        cp.id AS customer_product_id,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM inventory_moves im
                WHERE im.customer_product_id = cp.id 
                AND im.operation_type IN ('RECEIPT', 'picking')
            ) 
            AND EXISTS (
                SELECT 1 FROM sale_items si
                WHERE si.customer_product_id = cp.id
            ) THEN 'Comprado via Lobby'
            WHEN EXISTS (
                SELECT 1 FROM inventory_moves im
                WHERE im.customer_product_id = cp.id 
                AND im.operation_type IN ('manual_input', 'customer_input')
            ) 
            OR NOT EXISTS (
                SELECT 1 FROM sale_items si
                WHERE si.customer_product_id = cp.id
            ) THEN 'Recebido diretamente do cliente'
            ELSE 'Desconhecido'
        END AS origem
    FROM customer_products cp
),
precos_completos AS (
    SELECT 
        p.id AS product_id,
        (pu.product_cost * pu.markup) AS preco_venda_total
    FROM products p 
    LEFT JOIN (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units ASC) AS row_rank
            FROM pricing_ranges 
            WHERE units >= 50
        ) pr WHERE row_rank = 1
    ) pu ON p.id = pu.product_id
),
enderecados AS (
    SELECT DISTINCT customer_product_id
    FROM customer_products_inventory_addresses
)
SELECT 
    c.name AS "Cliente",
    tp.team_name AS "Time",
    s.code AS "Pedido",
    pc.name AS "Categoria",
    COALESCE(p.name, cp.name) AS "Nome",
    COALESCE(p.sku, cp.sku) AS "SKU",
    CASE 
        WHEN COALESCE(p.ncm, cp.ncm) = '.' THEN NULL 
        ELSE COALESCE(p.ncm, cp.ncm) 
    END AS "NCM",
    COALESCE(cps.name, 'Sem tamanho') AS "Tamanho",
    GREATEST(0, SUM(
        CASE 
            WHEN cps.id IS NOT NULL THEN COALESCE(cps.inventory_amount, 0) 
            ELSE COALESCE(cp.unique_inventory_amount, 0) 
        END
    )) AS "Estoque",
    GREATEST(0, SUM(
        CASE 
            WHEN cps.id IS NOT NULL THEN COALESCE(cps.inventory_amount, 0) * 
                COALESCE(cp.unit_price, pcx.preco_venda_total, 0)
            ELSE COALESCE(cp.unique_inventory_amount, 0) * 
                COALESCE(cp.unit_price, pcx.preco_venda_total, 0)
        END
    )) AS "Valor Estoque (R$)",
    po.origem AS "Origem",
    COALESCE(
        (SELECT SUM(im.quantity) 
         FROM inventory_moves im 
         WHERE im.customer_product_id = cp.id 
           AND (cps.id IS NULL OR im.variant_id = cps.id)
        ), 0
    ) AS "Quantidade Movimentada",
    COALESCE(sd.total_shipped, 0) AS "Envios 90 dias",
    CASE 
        WHEN e.customer_product_id IS NOT NULL THEN 'Endereçado'
        ELSE 'Não Endereçado'
    END AS "Endereçado"
FROM customer_products cp
LEFT JOIN customer_product_sizes cps ON cps.customer_product_id = cp.id
LEFT JOIN products p ON cp.product_id = p.id
LEFT JOIN precos_completos pcx ON p.id = pcx.product_id
LEFT JOIN product_categories pc ON p.category_id = pc.id
LEFT JOIN companies c ON c.id = cp.company_id
LEFT JOIN team_per_company tp ON tp.company_id = c.id
LEFT JOIN sale_items si ON si.customer_product_id = cp.id
LEFT JOIN sales s ON s.id = si.sale_id
LEFT JOIN sale_item_production_infos sipi ON sipi.sale_item_id = si.id
LEFT JOIN product_origins po ON po.customer_product_id = cp.id
LEFT JOIN enderecados e ON e.customer_product_id = cp.id
LEFT JOIN (
    SELECT 
        c.name AS client_name,
        cp.sku AS sku,
        COALESCE(sh.shirt_size, 'Sem tamanho') AS size_name,
        SUM(si.quantity) AS total_shipped
    FROM shipments sh
    LEFT JOIN shipping_orders so ON sh.shipping_order_id = so.id
    LEFT JOIN companies c ON so.company_id = c.id
    LEFT JOIN shipment_items si ON sh.id = si.shipment_id
    LEFT JOIN customer_products cp ON si.customer_product_id = cp.id
    WHERE 
        so.status <> 'canceled'
        AND sh.deleted_at IS NULL
        AND so.deleted_at IS NULL
        AND so.shipment_expected_to >= CURRENT_DATE - INTERVAL '90 days'
        AND sh.shipment_category IN ('SHIPPING', 'DEVOLUTION_RESEND')
    GROUP BY c.name, cp.sku, COALESCE(sh.shirt_size, 'Sem tamanho')
) sd ON sd.client_name = c.name 
     AND sd.sku = COALESCE(p.sku, cp.sku)
     AND sd.size_name = COALESCE(cps.name, 'Sem tamanho')
WHERE 
    (COALESCE(cp.unique_inventory_amount, 0) > 0 
     OR COALESCE(cps.inventory_amount, 0) > 0 
     OR COALESCE(sd.total_shipped, 0) > 0)
GROUP BY 
    c.name, tp.team_name, s.code, pc.name, p.name, cp.name, p.sku, cp.sku, p.ncm, cp.ncm, cp.id, cps.id, po.origem, pcx.preco_venda_total, sd.total_shipped, e.customer_product_id
ORDER BY 
    c.name, tp.team_name, s.code, COALESCE(p.name, cp.name);
