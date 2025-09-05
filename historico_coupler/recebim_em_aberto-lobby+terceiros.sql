WITH sub_recebimentos AS (
    SELECT 
        parent_id,
        SUM(expected_quantity) AS soma_quantidade
    FROM inventory_moves
    WHERE 
        operation_type IN ('RECEIPT', 'customer_input')
        AND status NOT IN ('COMPLETED', 'CANCELED')
    GROUP BY parent_id
),

company_plans AS (
    SELECT 
        c.id AS company_id,
        COALESCE(p.name, 'Sem Plano') AS plan_name
    FROM companies c
    LEFT JOIN subscriptions sub ON c.id = sub.company_id  
    LEFT JOIN plans p ON sub.plan_id = p.id  
)

SELECT  
    -- Datas
    to_char(im.created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS "Data_criacao",
    to_char(im.estimated_arrival_date AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS "data_estimada_recebimento",

    -- Tipo de recebimento
    CASE 
        WHEN im.operation_type = 'RECEIPT' THEN 'Recebimento Lobby' 
        ELSE 'Recebimento Terceiro' 
    END AS "Tipo Recebimento",

    -- Empresa e produto
    c."name" AS "Empresa",
    cp_info.plan_name AS "Plano",
    cp.sku AS "SKU",
    CASE 
        WHEN cp.product_id IS NOT NULL THEN p.name 
        ELSE cp."name"
    END AS "Produto",
    pc.name AS "Categoria",

    -- Observação e código da venda
    cp.internal_note AS "Obs_interna",
    im.reference_code AS "Venda",

    -- Quantidade
    COALESCE(sr.soma_quantidade, COALESCE(im.quantity, im.expected_quantity)) AS "Quantidade",

    -- Valor unitário
    cp.unit_price AS "Valor_unitario",

    -- Valor total
    COALESCE(sr.soma_quantidade, COALESCE(im.quantity, im.expected_quantity)) * cp.unit_price AS "Valor_total",

    -- Status
    CASE 
        WHEN im.status = 'TO_CHECK' THEN 'Aguardando Verificação'
        WHEN im.status = 'TO_PROCESS' THEN 'Para Receber'
        WHEN im.status = 'CANCELED' THEN 'Cancelado'
        WHEN im.status = 'COMPLETED' THEN 'Concluído'
        WHEN im.status = 'FAILURE' THEN 'Com Divergência'
        ELSE im.status
    END AS "Status",

    -- Comentários
    im.notes AS "Comentários"

FROM inventory_moves im
LEFT JOIN customer_products cp ON im.customer_product_id = cp.id 
LEFT JOIN companies c ON cp.company_id = c.id
LEFT JOIN products p ON cp.product_id = p.id
LEFT JOIN product_categories pc ON pc.id = p.category_id
LEFT JOIN sub_recebimentos sr ON sr.parent_id = im.id
LEFT JOIN company_plans cp_info ON cp_info.company_id = c.id

WHERE 
    im.status NOT IN ('COMPLETED', 'CANCELED')
    AND im.operation_type IN ('RECEIPT', 'customer_input')

ORDER BY 
    c."name", cp_info.plan_name, pc.name, 
    CASE 
        WHEN cp.product_id IS NOT NULL THEN p.name 
        ELSE cp."name"
    END;
