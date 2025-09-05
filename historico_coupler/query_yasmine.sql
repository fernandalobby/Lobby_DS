--- QUERY YASMINE
SELECT
    -- Cliente
    c.name AS client_name,
    c.cnpj,

    -- Vendas
    s.omie_order_number,
    s.code,
    s.id,
    s.company_id,
    (s.created_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS created_at,
    (s.order_date AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date AS order_date,
    au_seller.first_name AS seller,
    au_designer.first_name AS designer,
    s.is_event,
    s.event_date,
    s.deleted_at,
    s.total_price,
    s.shipment_price,
    s.storage_price,
    s.additional_value,
    s.additional_value_description,
    s.discount_value,
    s.discount_description,
    s.sold_kits_amount,
    CONCAT('https://admin.lobby.tech/vendas/', s.id) AS sale_link,
    s.payment_method,
    s.number_of_installments,
    s.storage,
    s.redeem_page,
    s.sample,
    s.status AS sale_status,

    -- Produto
    p.name AS product_name,
    p.sku AS product_sku,
    pc.name AS product_category,

    -- Item
    si.status as item_status,
    CAST (si.final_price AS DECIMAL(10,2)),
    CAST ((si.final_price * si.amount) AS DECIMAL(10,2)) as total_final_price,
	CAST(sipi.unit_cost as DECIMAL(10,2)),
    CAST(((sipi.exceeding_quantity + sipi.purchased_quantity) * sipi.unit_cost) as DECIMAL(10,2)) as total_cost,
    CAST (sipi.customize_cost as DECIMAL(10,2)),
	CAST (((sipi.exceeding_quantity + sipi.purchased_quantity) * sipi.customize_cost) AS DECIMAL(10,2)) as total_customize_cost,

    -- Suppliers
    ss.trading_name AS supplier,
    sc.trading_name AS customizer,

    -- Frete
    shipments.shipment_cost,
    shipments.shipment_price,

    -- Quotes
    q.total_price AS quote_total_price,
    q.shipment_price AS quote_shipment_price,
    q.storage_price AS quote_storage_price,
    q.additional_value AS quote_additional_value,
    q.additional_value_description AS quote_additional_value_description,
    q.discount_value AS quote_discount_value,
    q.discount_description AS quote_discount_description,
    q.turbo,
    q.budget_per_kit,
    CAST(sipi.purchased_quantity AS FLOAT) "client_quantity", 
 CAST(sipi.exceeding_quantity AS FLOAT), 
 CAST((sipi.purchased_quantity + sipi.exceeding_quantity) AS FLOAT) "production_quantity"
FROM
    sales s
LEFT JOIN
    companies c ON c.id = s.company_id
LEFT JOIN
    sale_items si ON s.id = si.sale_id
LEFT JOIN
    products p ON p.id = si.product_id
LEFT JOIN
    product_categories pc ON pc.id = p.category_id
LEFT JOIN
    sale_item_production_infos sipi ON si.id = sipi.sale_item_id
LEFT JOIN
    sale_item_design_infos sidi ON si.id = sidi.sale_item_id
LEFT JOIN
    sale_item_images sii ON si.id = sii.sale_item_id
--LEFT JOIN
--    inventory_moves im ON si.id = im.sale_item_id
LEFT JOIN
    suppliers ss ON sipi.product_supplier_id = ss.id
LEFT JOIN
    suppliers sc ON sipi.product_customizer_id = sc.id
LEFT JOIN
    admin_users au_seller ON s.seller_id::bigint = au_seller.id
LEFT JOIN
    admin_users au_designer ON s.designer_id::bigint = au_designer.id
LEFT JOIN
    shipments ON s.id = shipments.id
LEFT JOIN
    quotes q ON s.id = q.id
WHERE
    s.created_at at time zone 'utc' AT TIME ZONE 'America/Sao_Paulo' BETWEEN '2025-01-01 00:00:00' AND '2025-05-14 23:59:59'
ORDER BY
    s.created_at DESC;
