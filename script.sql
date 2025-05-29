
CREATE DATABASE il_levels_demo;
USE il_levels_demo;

DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    account_holder VARCHAR(255) NOT NULL,
    balance DECIMAL(10, 2) NOT NULL
);

INSERT INTO accounts (account_holder, balance) VALUES
('Alice', 1000.00),
('Bob', 500.00);

SELECT * FROM accounts;

drop table if exists seats;

create table seats (
    seat_id INT auto_increment primary key,
    status ENUM('available', 'booked') NOT NULL
);

insert into seats (status) values
	('booked'),
	('booked'),
	('available'),
	('booked');

select * from seats;

