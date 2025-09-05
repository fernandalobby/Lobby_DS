SELECT
    p.sku,
    p.name AS product_name,
    sup.trading_name AS supplier_name,
    pr.units,
    pr.product_cost,
    pr.markup,
    p.credit_tax,
    p.substitution_tax
FROM pricing_ranges pr
JOIN products p ON p.id = pr.product_id
LEFT JOIN suppliers sup ON sup.id = p.default_supplier_id
WHERE LOWER(sup.trading_name) NOT IN ('spot', 'asia', 'xbz');
