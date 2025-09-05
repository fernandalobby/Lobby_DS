SELECT 
    -- Nome do Produto do Cliente (mesmo sem SKU)
    cp.name AS "Nome Produto Cliente",
    -- SKU oficial (pode ser NULL se não tiver no sistema)
    p.sku AS "SKU Cadastrado",
    -- Nome oficial (se existir)
    p.name AS "Nome Produto Oficial",
    -- Data da Venda
    TO_CHAR(s.created_at AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD') AS "Data da Venda",
    -- Cliente
    c.name AS "Cliente",
    -- Quantidade Vendida
    sipi.purchased_quantity AS "Quantidade Vendida",
    -- Valor da Venda
    si.final_price * sipi.purchased_quantity AS "Valor da Venda",
    -- Link do Fornecedor
    si.product_supplier_link AS "Link do Fornecedor"
FROM 
    sales s
JOIN 
    sale_items si ON s.id = si.sale_id
LEFT JOIN 
    sale_item_production_infos sipi ON si.id = sipi.sale_item_id
LEFT JOIN 
    customer_products cp ON si.customer_product_id = cp.id
LEFT JOIN 
    products p ON cp.product_id = p.id
LEFT JOIN 
    companies c ON s.company_id = c.id
WHERE 
    cp.name IS NOT NULL  -- garante que temos um nome do cliente
    AND (p.sku IS NULL OR p.sku = '')  -- filtra apenas produtos SEM SKU
ORDER BY 
    "Data da Venda" DESC, "Cliente";
