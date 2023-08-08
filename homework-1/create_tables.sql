-- SQL-команды для создания таблиц
CREATE TABLE IF NOT EXISTS customers
(
    customer_id char(5) PRIMARY KEY,
    company_name varchar(100) NOT NULL,
    contact_name varchar(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employees
(
    employee_id int PRIMARY KEY,
    first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	title varchar(100) NOT NULL,
    birth_date date NOT NULL,
	notes text
);

CREATE TABLE IF NOT EXISTS orders
(
	order_id int PRIMARY KEY,
	customer_id char(5) REFERENCES customers(customer_id) ON DELETE CASCADE NOT NULL,
	employee_id int REFERENCES employees(employee_id) ON DELETE CASCADE NOT NULL,
	order_date date NOT NULL,
	ship_city varchar(20) NOT NULL
);