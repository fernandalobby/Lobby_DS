WITH pricing_units AS (
    SELECT 
        pr.product_id,
        pr.units::numeric AS units,
        pr.markup::numeric AS markup,
        pr.product_cost::numeric AS product_cost
    FROM pricing_ranges pr
    WHERE pr.units IN (50, 100, 1000)
),
customization_prices AS (
    SELECT 
        pctp.product_id,
        pcp.quantity,
        pcp.price::numeric AS price,
        pcp.markup::numeric AS markup
    FROM product_customization_prices pcp
    INNER JOIN product_customization_types t ON pcp.product_customization_type_id = t.id
    INNER JOIN product_customization_types_products pctp ON t.id = pctp.product_customization_type_id
    WHERE pcp.quantity IN (50, 100, 1000)
),
combined AS (
    SELECT 
        p.sku,
        p.name,
        p.short_description,
        p.image_data::jsonb->>'id' AS image_id,
        pu.units,
        pu.product_cost,
        pu.markup AS product_markup,
        cp.price AS customization_price,
        cp.markup AS customization_markup,
        (pu.product_cost * pu.markup) + (cp.price * cp.mxarkup) AS total_price
    FROM products p
    INNER JOIN pricing_units pu ON p.id = pu.product_id
    INNER JOIN customization_prices cp 
        ON p.id = cp.product_id AND pu.units = cp.quantity
     WHERE p.status = 'ACTIVE'
),
min_prices AS (
    SELECT
        sku,
        name,
        short_description,
        image_id,
        units,
        MIN(total_price) AS min_price
    FROM combined
    GROUP BY sku, name, short_description, image_id, units
)
SELECT 
    sku AS "SKU",
    name AS "Nome",
    short_description AS "Descrição Curta",
    MAX(CASE WHEN units = 50 THEN min_price END) AS "Preço 50 unid.",
    MAX(CASE WHEN units = 100 THEN min_price END) AS "Preço 100 unid.",
    MAX(CASE WHEN units = 1000 THEN min_price END) AS "Preço 1000 unid.",
'https://res.cloudinary.com/hiwtbwecx/image/upload/c_limit,q_auto,w_240/v1/store/' || image_id AS "Imagem"
FROM min_prices
GROUP BY sku, name, short_description, image_id
ORDER BY sku, name;
