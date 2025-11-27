-- Base schema for nyc_apartments
CREATE TABLE IF NOT EXISTS listings (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,          -- Internal DB ID
  source VARCHAR(32) NOT NULL,                   -- 'streeteasy'
  external_id VARCHAR(128) NOT NULL,             -- The 'id' from the JSON node
  area_name VARCHAR(128),
  available_at DATE,
  bedroom_count DECIMAL(3,1),
  building_type VARCHAR(32),
  full_bathroom_count INT,
  furnished BOOLEAN,
  latitude DECIMAL(10,7),
  longitude DECIMAL(10,7),
  half_bathroom_count INT,
  has_tour_3d BOOLEAN,
  has_videos BOOLEAN,
  interesting_price_delta INT,
  is_new_development BOOLEAN,
  lease_term INT,
  living_area_size INT,
  months_free INT,
  net_effective_price INT,
  off_market_at DATETIME,
  price INT,
  rello_express VARCHAR(64),
  slug VARCHAR(255),
  source_group_label VARCHAR(255),
  source_type VARCHAR(32),
  state VARCHAR(16),
  status VARCHAR(32),
  street VARCHAR(255),                           -- This is the new 'address'
  upcoming_open_house_start_time DATETIME,
  display_unit VARCHAR(64),
  url_path VARCHAR(255),                         -- This can be used to build the full URL
  zip_code VARCHAR(16),
  tier VARCHAR(64),
  -- Use API id (external_id) together with source as natural key for upsert
  unit VARCHAR(64),
  date_added DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  date_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_source_external (source, external_id)
);

CREATE TABLE IF NOT EXISTS neighborhoods (
  id INT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  slug VARCHAR(128),
  level INT,
  parent_id INT,
  UNIQUE KEY uniq_name (name)
);

-- Add foreign key from `listings` to `neighborhoods`
ALTER TABLE listings ADD CONSTRAINT fk_area_name
FOREIGN KEY (area_name) REFERENCES neighborhoods(name);
