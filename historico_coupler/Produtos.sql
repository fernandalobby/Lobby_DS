WITH sales_data AS (
    SELECT 
        si.product_id,
        SUM(sipi.purchased_quantity) AS total_quantity_sold,
        SUM(si.final_price * sipi.purchased_quantity) AS total_value_sold
    FROM 
        sales s
    JOIN 
        sale_items si ON s.id = si.sale_id
    LEFT JOIN 
        sale_item_production_infos sipi ON si.id = sipi.sale_item_id
    WHERE 
        s.created_at >= (NOW() - INTERVAL '6 months')
    GROUP BY 
        si.product_id
),
quotes_data AS (
    SELECT
        qi.product_id,
        SUM(qi.amount) AS total_quantity_quoted,
        SUM(qi.amount * qi.final_price) AS total_quotes
    FROM 
        quotes q
    LEFT JOIN 
        quote_items qi ON q.id = qi.quote_id
    WHERE 
        q.created_at >= (NOW() - INTERVAL '6 months')
    GROUP BY 
        qi.product_id
),
pricing_units AS (
    SELECT 
        pr.product_id,
        pr.units::numeric AS units,
        REPLACE(CAST(pr.markup AS TEXT), '.', ',') AS markup,
        REPLACE(CAST(pr.personalization_cost AS TEXT), '.', ',') AS personalization_cost,
        REPLACE(CAST(pr.product_cost AS TEXT), '.', ',') AS product_cost,
        ROW_NUMBER() OVER (
            PARTITION BY pr.product_id 
            ORDER BY 
                CASE 
                    WHEN pr.units = 50 THEN 1 
                    ELSE 2 
                END,
                pr.units ASC 
        ) AS row_rank 
    FROM 
        pricing_ranges pr
),
product_images_ranked AS (
    SELECT 
        pi.product_id,
        pi.image_data,
        ROW_NUMBER() OVER (PARTITION BY pi.product_id ORDER BY pi.created_at ASC) AS row_rank
    FROM 
        product_images pi
)
SELECT DISTINCT
    p.name,
    CAST(pir.image_data AS TEXT) AS image_data,
    p.created_at,
    p.updated_at,
    pc.name AS category_name,
    p.sku,
    p.description,
    p.short_description,
    p.mockup_link,
    p.template_link,
    p.internal_note,
    CAST(p.tags AS TEXT) AS tag,
    p.status,
    p.ncm,
    p.sizes_grid_id,
    p.brand_id,
    p.handling_range_size_id,
    sup_supplier.trading_name AS default_supplier_name,
    sup_customizer.trading_name AS default_customizer_name,
    p.id AS product_id,
    STRING_AGG(DISTINCT g.name, ', ') AS group_names,
    STRING_AGG(DISTINCT CONCAT(g.name, ' (', CASE WHEN g.active THEN 'Ativo' ELSE 'Inativo' END, ')'), ', ') AS group_status,
    pr_final.units,
    pr_final.markup, 
    pr_final.personalization_cost,
    pr_final.product_cost,
    COALESCE(sd.total_quantity_sold, 0) AS total_quantity_sold,
    COALESCE(sd.total_value_sold, 0) AS total_value_sold,
    COALESCE(qd.total_quantity_quoted, 0) AS total_quantity_quoted,
    COALESCE(qd.total_quotes, 0) AS total_quotes,
    p.production_time,
    p.turbo_production_time
FROM 
    products p
LEFT JOIN 
    product_categories pc ON p.category_id = pc.id
LEFT JOIN 
    suppliers sup_supplier ON p.default_supplier_id = sup_supplier.id
LEFT JOIN 
    suppliers sup_customizer ON p.default_customizer_id = sup_customizer.id
LEFT JOIN 
    products_groups pg ON p.id = pg.product_id
LEFT JOIN 
    groups g ON pg.group_id = g.id
LEFT JOIN 
    sales_data sd ON p.id = sd.product_id
LEFT JOIN 
    quotes_data qd ON p.id = qd.product_id
LEFT JOIN 
    (SELECT * FROM pricing_units WHERE row_rank = 1) pr_final ON p.id = pr_final.product_id
LEFT JOIN 
    (SELECT * FROM product_images_ranked WHERE row_rank = 1) pir ON p.id = pir.product_id
GROUP BY 
    p.name, 
    pc.name, 
    pir.image_data::TEXT,
    p.created_at, 
    p.updated_at, 
    p.sku, 
    p.description, 
    p.short_description, 
    p.mockup_link, 
    p.template_link, 
    p.internal_note, 
    p.tags::TEXT,
    p.status, 
    p.ncm, 
    p.sizes_grid_id, 
    p.brand_id, 
    p.handling_range_size_id, 
    sup_supplier.trading_name, 
    sup_customizer.trading_name, 
    p.id, 
    pr_final.units, 
    pr_final.markup, 
    pr_final.personalization_cost, 
    pr_final.product_cost, 
    sd.total_quantity_sold, 
    sd.total_value_sold,
    qd.total_quantity_quoted,
    qd.total_quotes,
    p.production_time,
    p.turbo_production_time
ORDER BY 
    p.sku;
