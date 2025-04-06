CREATE TABLE toll_data (
    rowid INTEGER PRIMARY KEY,
    timestamp TIMESTAMP,
    anonymized_vehicle_number INTEGER,
    vehicle_type VARCHAR(50),
    number_of_axles INTEGER,
    tollplaza_id INTEGER,
    tollplaza_code VARCHAR(10),
    type_of_payment_code VARCHAR(3),
    vehicle_code VARCHAR(5)
);