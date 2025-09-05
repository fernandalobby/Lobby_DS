WITH UltimaCompra AS (
    SELECT DISTINCT ON (s.company_id) 
        s.company_id,
        s.id AS venda_id,
        s.order_date AS data_ultima_compra,
        s.total_price AS valor_ultima_compra
    FROM sales s
    WHERE s.status <> 'CANCELED'
    ORDER BY s.company_id, s.order_date DESC  
),

Plano AS( 
	SELECT DISTINCT ON (s.company_id) 
		s.company_id,
		p.name
    FROM 
        sales s
    JOIN 
        companies c ON s.company_id = c.id
    LEFT JOIN 
        subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN 
        plans p ON sub.plan_id = p.id  
    ORDER BY s.company_id, sub.created_at DESC
),

Receita2025 AS (
    SELECT 
        s.company_id,
        SUM(s.total_price) AS receita_produtos_2025
    FROM sales s
    WHERE EXTRACT(YEAR FROM s.order_date) = 2025
    GROUP BY s.company_id
),

Receita2024 AS (
    SELECT 
        s.company_id,
        SUM(s.total_price) AS receita_produtos_2024
    FROM sales s
    WHERE EXTRACT(YEAR FROM s.order_date) = 2024
    GROUP BY s.company_id
),

ReceitaTotal AS (
    SELECT 
        s.company_id,
        SUM(s.total_price) AS receita_total_produtos,
        COUNT(s.id) AS numero_compras,
        AVG(s.total_price) AS ticket_medio
    FROM sales s
    GROUP BY s.company_id
),

UltimoEnvio AS (
    SELECT 
        so.company_id,
        MAX(so.created_at) AS data_ultimo_envio
    FROM shipping_orders so
    GROUP BY so.company_id
),

EstoqueCliente AS (
    SELECT 
        c.id AS company_id,
        SUM(
            CASE 
                WHEN cps.id IS NOT NULL THEN COALESCE(cps.inventory_amount, 0)
                ELSE COALESCE(cp.unique_inventory_amount, 0)
            END
        ) AS estoque_total
    FROM customer_products cp
    LEFT JOIN customer_product_sizes cps ON cps.customer_product_id = cp.id
    JOIN companies c ON c.id = cp.company_id
    WHERE 
        (cp.unique_inventory_amount > 0 OR cps.inventory_amount > 0)
    GROUP BY c.id
),

ConsumoMensal AS (
    SELECT
        so.company_id,
        SUM(si.quantity) AS total_enviado,
        COUNT(DISTINCT DATE_TRUNC('month', so.created_at)) AS meses_com_envios,
        CASE 
            WHEN COUNT(DISTINCT DATE_TRUNC('month', so.created_at)) > 0 
            THEN ROUND(SUM(si.quantity)::numeric / COUNT(DISTINCT DATE_TRUNC('month', so.created_at)), 2)
            ELSE 0
        END AS consumo_medio_mensal
    FROM shipping_orders so
    JOIN shipments sh ON sh.shipping_order_id = so.id
    JOIN shipment_items si ON si.shipment_id = sh.id
    WHERE 
        so.created_at >= NOW() - INTERVAL '12 months'
        AND so.status <> 'canceled'
        AND sh.deleted_at IS NULL
        AND so.deleted_at IS NULL
        AND sh.shipment_category IN ('SHIPPING', 'DEVOLUTION_RESEND')
    GROUP BY so.company_id
)

SELECT 
    c.name AS "Cliente",
    p.name as "Plano",
    TO_CHAR(uc.data_ultima_compra, 'DD/MM/YYYY') AS "Data Última Compra",
    uc.valor_ultima_compra AS "Valor Última Compra",
    'https://admin.lobby.tech/vendas/' || uc.venda_id AS "Link Última Compra",
    r24.receita_produtos_2024 as "Receita 2024",
    r25.receita_produtos_2025 as "Receita 2025",
    rt.receita_total_produtos as "Receita Total",
    rt.numero_compras as "Quant. Compras",
    rt.ticket_medio  AS "Ticket Médio",
    TO_CHAR(ue.data_ultimo_envio, 'DD/MM/YYYY') AS "Data Último Envio",
    ec.estoque_total AS "Estoque",
    CASE 
        WHEN cm.consumo_medio_mensal > 0 
        THEN ROUND(ec.estoque_total::numeric / cm.consumo_medio_mensal, 1)
        ELSE NULL
    END AS "Giro"
FROM companies c
LEFT JOIN UltimaCompra uc ON c.id = uc.company_id
LEFT JOIN Plano p ON c.id = p.company_id
LEFT JOIN Receita2025 r25 ON c.id = r25.company_id
LEFT JOIN Receita2024 r24 ON c.id = r24.company_id
LEFT JOIN ReceitaTotal rt ON c.id = rt.company_id
LEFT JOIN UltimoEnvio ue ON c.id = ue.company_id
LEFT JOIN EstoqueCliente ec ON c.id = ec.company_id
LEFT JOIN ConsumoMensal cm ON c.id = cm.company_id
ORDER BY c.name;
