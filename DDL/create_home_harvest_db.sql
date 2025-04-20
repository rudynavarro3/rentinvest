CREATE DATABASE homeharvest;

-- Property status lookup table
CREATE TABLE IF NOT EXISTS property_status (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(20) UNIQUE NOT NULL
);

-- Property style lookup table
CREATE TABLE IF NOT EXISTS property_style (
    style_id SERIAL PRIMARY KEY,
    style_name VARCHAR(50) UNIQUE NOT NULL
);

-- Location table
CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(20) NOT NULL,
    state VARCHAR(10) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    UNIQUE (city, state, zip_code)
);

-- Main properties table
CREATE TABLE IF NOT EXISTS properties (
    property_id SERIAL PRIMARY KEY,
    property_url VARCHAR(255) NOT NULL,
    mls VARCHAR(10) NOT NULL,
    mls_id VARCHAR(50) NOT NULL,
    status_id INTEGER REFERENCES property_status(status_id),
    style_id INTEGER REFERENCES property_style(style_id),
    street VARCHAR(255) NOT NULL,
    unit VARCHAR(50),
    location_id INTEGER REFERENCES locations(location_id),
    beds SMALLINT,
    full_baths SMALLINT,
    half_baths SMALLINT,
    sqft INTEGER,
    year_built INTEGER,
    days_on_mls SMALLINT,
    list_price INTEGER,
    list_date DATE,
    sold_price INTEGER,
    last_sold_date DATE,
    lot_sqft INTEGER,
    price_per_sqft INTEGER,
    latitude REAL,
    longitude REAL,
    stories INTEGER,
    hoa_fee SMALLINT,
    parking_garage SMALLINT,
    primary_photo VARCHAR(255),
    alt_photos TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_url, mls_id)
);

-- Create indexes for frequently queried columns
CREATE INDEX idx_properties_status ON properties(status_id);
CREATE INDEX idx_properties_location ON properties(location_id);
CREATE INDEX idx_properties_mls ON properties(mls);
CREATE INDEX idx_properties_price ON properties(list_price);
CREATE INDEX idx_properties_sqft ON properties(sqft);

-- Insert initial status values
INSERT INTO property_status (status_name) VALUES 
    ('SOLD'),
    ('SELLING'),
    ('RENTING'),
    ('PENDING');

-- Create a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_properties_updated_at
    BEFORE UPDATE ON properties
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
