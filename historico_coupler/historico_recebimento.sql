WITH sub_recebimentos AS (
    SELECT 
        parent_id,
        SUM(expected_quantity) AS soma_quantidade
    FROM inventory_moves
    WHERE 
        operation_type IN ('RECEIPT', 'customer_input')
        AND status NOT IN ('CANCELED')
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
    to_char(im.arrived_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY') AS "Data_chegada",
    to_char(im.arrived_at AT TIME ZONE 'America/Sao_Paulo', 'MM/YYYY') AS "Mes_ano_chegada",
    -- Tipo de recebimento
    CASE 
        WHEN im.operation_type = 'RECEIPT' THEN 'Recebimento Lobby' 
        ELSE 'Recebimento Terceiro' 
    END AS "Tipo_Recebimento",
    -- Empresa e produto
    c.name AS "Empresa",
    cp.sku AS "SKU",
    COALESCE(p.name, cp.name) AS "Produto",
    pc.name AS "Categoria",
    -- Observações
    cp.internal_note AS "Venda",
    -- Quantidade e valores
    COALESCE(sr.soma_quantidade, 
             COALESCE(im.quantity, im.expected_quantity)) AS "Quantidade",
    cp.unit_price AS "Valor_unitario",
    COALESCE(sr.soma_quantidade, 
             COALESCE(im.quantity, im.expected_quantity)) * cp.unit_price AS "Valor_total",
    -- Fornecedores
    ss.trading_name AS "Fornecedor",
    sc.trading_name AS "Fornecedor de Personalizacao",
    -- Status
    CASE 
        WHEN im.status = 'TO_CHECK' THEN 'Aguardando Verificação'
        WHEN im.status = 'TO_PROCESS' THEN 'Para Receber'
        WHEN im.status = 'CANCELED' THEN 'Cancelado'
        WHEN im.status = 'COMPLETED' THEN 'Concluído'
        WHEN im.status = 'FAILURE' THEN 'Com Divergência'
        ELSE im.status
    END AS "Status",
    -- Comentários e plano
    im.notes AS "Comentários",
    cp_info.plan_name AS "Plano"
FROM inventory_moves im
LEFT JOIN customer_products cp ON im.customer_product_id = cp.id 
LEFT JOIN sale_items si ON im.sale_item_id = si.id
LEFT JOIN sale_item_production_infos sipi ON si.id = sipi.sale_item_id
LEFT JOIN suppliers ss ON sipi.product_supplier_id = ss.id
LEFT JOIN suppliers sc ON sipi.product_customizer_id = sc.id
LEFT JOIN companies c ON cp.company_id = c.id
LEFT JOIN products p ON cp.product_id = p.id
LEFT JOIN product_categories pc ON pc.id = p.category_id
LEFT JOIN sub_recebimentos sr ON sr.parent_id = im.id
LEFT JOIN company_plans cp_info ON cp_info.company_id = c.id
WHERE 
    im.status = 'COMPLETED'
    AND im.operation_type IN ('RECEIPT', 'customer_input')
    AND im.arrived_at IS NOT NULL
    AND to_char(im.arrived_at AT TIME ZONE 'America/Sao_Paulo', 'YYYYMM')::int >= 202310
ORDER BY im.updated_at DESC;
