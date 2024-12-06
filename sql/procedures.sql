delimiter //

CREATE PROCEDURE customer_name (IN c_id SMALLINT, OUT c_name VARCHAR(255) )
       BEGIN
         SELECT CONCAT(first_name, " ", last_name) INTO c_name FROM customers
         WHERE customer_id = c_id;
       END//


DROP PROCEDURE yearly_product_information;
CREATE PROCEDURE yearly_product_information (IN m_year SMALLINT)
BEGIN
	SELECT A.product_name, A.list_price, A.brand_name, A.category_name, B.total_stock
	FROM (	SELECT products.product_id, products.product_name, products.list_price, brands.brand_name, categories.category_name FROM products
			JOIN brands
			ON products.brand_id = brands.brand_id
			JOIN categories
			ON products.category_id = categories.category_id
			WHERE products.model_year = m_year
			) AS A
	JOIN (	SELECT product_id, SUM(quantity) as total_stock
			FROM stocks
			GROUP BY product_id
			) AS B
	ON A.product_id = B.product_id;
END//


CREATE PROCEDURE  store_inventory (IN i_store_id SMALLINT)
BEGIN
	SELECT products.product_name, stocks.quantity, products.list_price, brands.brand_name, categories.category_name FROM products
	JOIN stocks
	ON products.product_id = stocks.product_id
	JOIN brands
	ON products.brand_id = brands.brand_id
	JOIN categories
	ON products.category_id = categories.category_id
	WHERE stocks.store_id = i_store_id
	ORDER BY quantity DESC, products.list_price DESC;
END//

delimiter ;
