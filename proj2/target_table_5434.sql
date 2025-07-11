-- create employees table

CREATE TABLE employees (
  emp_id SERIAL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT
);

select * from employees;

delete from employees 