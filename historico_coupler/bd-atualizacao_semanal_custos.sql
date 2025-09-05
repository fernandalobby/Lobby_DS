SELECT 
    p.sku,
    p.internal_note,
    pr.units AS pricing_units,
    pr.product_cost,
    pr.markup,
    sup_supplier.trading_name AS default_supplier_name,
    MIN(TRIM(SPLIT_PART(sp.sku, '-', 1))) AS supplier_sku
FROM 
    products p
JOIN 
    pricing_ranges pr ON p.id = pr.product_id
LEFT JOIN 
    supplier_products sp ON p.id = sp.product_id
LEFT JOIN 
    suppliers sup_supplier ON p.default_supplier_id = sup_supplier.id
WHERE 
    p.status = 'ACTIVE'
GROUP BY 
    p.sku,
    p.internal_note,
    pr.units,
    pr.product_cost,
    pr.markup,
    sup_supplier.trading_name;
