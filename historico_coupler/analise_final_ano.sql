SELECT
    s.code,
    p.sku,
    p.name AS "produto",
    pc.name AS "categoria",
    si.amount AS "quantidade vendida",
    CAST(si.amount * si.final_price AS FLOAT) * dtp.variacao AS "valor de venda",
    q.kit_purpose AS "objetivo do orçamento",
    (s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS "created_at"
FROM
    sales s
LEFT JOIN
    shipping_orders so ON s.id = so.id
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
    quotes q ON s.quote_id = q.id
LEFT JOIN (
    SELECT
        s.code,
        s.total_price AS "venda_total_final",
        CAST(SUM(si.amount * si.final_price) AS FLOAT) AS "VENDA",
        CASE 
            WHEN SUM(si.amount * si.final_price) <= 0 THEN 1 
            ELSE s.total_price 
        END / 
        CASE 
            WHEN SUM(si.amount * si.final_price) <= 0 THEN 1 
            ELSE CAST(SUM(si.amount * si.final_price) AS FLOAT) 
        END AS variacao
    FROM
        sales s
    LEFT JOIN
        sale_items si ON s.id = si.sale_id
    WHERE
        s.status NOT IN ('CANCELED')
        AND si.status NOT IN ('CANCELED')
    GROUP BY
        s.total_price, s.code
) dtp ON s.code = dtp.code
LEFT JOIN (
    SELECT
        max(created_at), sale_item_id, arrived_at, expected_quantity, quantity, scrap_quantity, exceeding_quantity
    FROM
        inventory_moves
    WHERE
        quantity IS NOT NULL
        AND concat(created_at, sale_item_id) IN (
            SELECT
                concat(res.created_at, res.sale_item_id)
            FROM (
                SELECT
                    max(created_at) AS created_at, sale_item_id
                FROM
                    inventory_moves
                GROUP BY
                    sale_item_id
            ) res
        )
    GROUP BY
        sale_item_id, arrived_at, expected_quantity, quantity, scrap_quantity, exceeding_quantity
) im ON si.id = im.sale_item_id
WHERE
    s.status IN ('TO_PRODUCE', 'PRODUCING', 'WAITING_PAYMENT', 'AVAILABLE_IN_INVENTORY')
    AND si.status NOT IN ('CANCELED')
    AND (
        ((s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN '2023-09-01' AND '2023-12-31')
        OR
        ((s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN '2024-09-01' AND '2024-12-31')
    )
    AND (CAST(si.amount * si.final_price AS FLOAT) * dtp.variacao) > 0
