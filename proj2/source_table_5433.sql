-- This is the source table

-- create employees table
CREATE TABLE employees (
  emp_id SERIAL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT
);


-- create CDC table
CREATE TABLE emp_cdc (
  emp_id SERIAL,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  dob DATE,
  city VARCHAR(100),
  salary INT,
  action VARCHAR(100)
);

-- create function for CDC to capture INSERT, UPDATE, DELETE
CREATE OR REPLACE FUNCTION capture_changes() RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'INSERT');
    RETURN NEW;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'UPDATE');
    RETURN NEW;
  ELSIF (TG_OP = 'DELETE') THEN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, salary, action)
    VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'DELETE');
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- add the trigger
CREATE TRIGGER emp_changes_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION capture_changes();

-- fix the duplicate key value 
-- Drop the Current Primary Key
ALTER TABLE emp_cdc DROP CONSTRAINT emp_cdc_pkey;
-- Add an Auto-Increment ID for Change Logs
ALTER TABLE emp_cdc ADD COLUMN change_id SERIAL PRIMARY KEY;

-- test the trigger
-- insert
INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('John', 'Doe', '1990-01-01', 'New York', 70000);
SELECT * FROM emp_cdc; -- action = 'INSERT'
-- update
UPDATE employees
SET city = 'Los Angeles', salary = 75000
WHERE first_name = 'John' AND last_name = 'Doe';
SELECT * FROM emp_cdc;  -- action = 'UPDATE'
-- delete the test code
DELETE FROM employees
WHERE first_name = 'Test4';
SELECT * FROM emp_cdc; 

-- more test cases
INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Test3', 'F', '1998-01-01', 'New Jersey', 80000);

INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Test4', 'F', '1998-01-01', 'New Jersey', 80000);

INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Test13', 'W', '1863-01-01', 'New Jersey', 90000);

UPDATE employees
SET first_name = 'NEW19', salary = 50000
WHERE first_name = 'Test19';

INSERT INTO employees (first_name, last_name, dob, city, salary)
VALUES ('Test21', 'W', '1947-05-01', 'Boston', 90000);




SELECT * FROM emp_cdc; 
SELECT * FROM employees; 






