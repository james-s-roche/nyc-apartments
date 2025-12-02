# NYC Apartments Analytics

## Overview

This project is a data pipeline and analysis tool for scraping, storing, and analyzing rental apartment listings from StreetEasy in New York City. It provides tools to ingest data into a MySQL database, visualize neighborhood data, and export listings to CSV. 

## Demo

Here is a brief demonstration of the interactive Dash web application, showcasing the map, table, and treemap views. (may take a minute to load)

![NYC Apartments App Demo](https://media.githubusercontent.com/media/james-s-roche/nyc-apartments/refs/heads/main/img/app_demo.gif)

## Features

- **Scrape Listings:** Scrape rental listings from StreetEasy for all neighborhoods.
- **Data Ingestion:** Ingest and normalize scraped data into a structured MySQL database.
- **Neighborhood Visualizations:** Generate interactive treemaps, sunburst charts, and icicle charts to visualize the hierarchy of NYC neighborhoods. 
- **Machine Learning:** Use scikit-learn to build models to predict apartment prices and identify good deals. 
- **Data Analysis and Viz** Analyze how prices and listings vary over time. Visualize with Choropleth maps.
- **Data Export:** Export listings data to a CSV file for further analysis.
- **CLI Interface:** A command-line interface to run the various components of the project.

## Project Structure

```
/
├── app/
│   ├── app.py
│   └── data_utils.py
│   └── pages/
│       ├── home.py
│       ├── table_view.py
│       └── treemap.py
├── config/
│   ├── .env.example
│   └── settings.py
├── data/
│   └── neighborhoods.json
├── database/
│   ├── add_slug_column.py
│   ├── mysql_client.py
│   ├── migrate.py
│   ├── schema.sql
│   └── utils.py
├── img/
│   ├── neighborhood_treemap.html
│   └── app_demo.gif
├── logs/
│   ├── processed_neighborhoods.txt
│   └── scraping.log
├── scraping/
│   ├── ingest_listings.py
│   ├── ingest_neighborhoods.py
│   ├── scrape_listings.py
│   └── streeteasy.py
├── tests/
│   └── test_ingest.py
├── tools/
│   ├── export_csv.py
│   ├── neighborhood_diagram.py
│   └── neighborhood_treemap.py
├── requirements.txt
└── README.md
```

## Installation

1.  **Clone the repository:**
    ```
    git clone https://github.com/your-username/nyc_apartments.git
    cd nyc_apartments
    ```

2.  **Create and activate a virtual environment:**
    This project uses Python 3.9.
    ```
    python3.9 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install the dependencies:**
    ```
    pip install -r requirements.txt
    ```

## Database Setup

1.  **Install MySQL:**
    Make sure you have a MySQL server running. You can install it using Homebrew on macOS:
    ```
    brew install mysql
    brew services start mysql
    ```

2.  **Create the database and user:**
    Log in to MySQL and run the following commands:
    ```sql
    CREATE DATABASE nyc_apartments CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE USER 'nyc_user'@'localhost' IDENTIFIED BY 'password';
    GRANT ALL PRIVILEGES ON nyc_apartments.* TO 'nyc_user'@'localhost';
    FLUSH PRIVILEGES;
    ```

3.  **Configure environment variables:**
    Copy the `config/.env.example` file to `.env` and update the values with your database credentials.
    ```
    cp config/.env.example .env
    ```

4.  **Apply the database schema:**
    ```
    python -m database.migrate
    ```

## Usage

### Scraping and Ingesting Data

1.  **Ingest Neighborhoods:**
    This script scrapes neighborhood data from StreetEasy and populates the `neighborhoods` table in the database.
    ```
    python -m scraping.ingest_neighborhoods
    ```

2.  **Scrape Listings:**
    This script scrapes apartment listings for all leaf neighborhoods and stores them in the database. It will automatically skip neighborhoods that have already been scraped.
    ```
    python -m scraping.scrape_listings --delay 15.0 --level 2
    ```


### Running the Web Application

This project includes an interactive dashboard built with Dash to explore the apartment data.

1.  **Navigate to the project root directory:**
    Make sure you are in the `nyc_apartments` directory and your virtual environment is activated.

2.  **Run the app:**
    Run the app as a module from the project's root directory. This ensures that the Python interpreter correctly recognizes the `app` folder as a package.
    ```
    python -m app.app
    ```

3.  **View the dashboard:**
    Open your web browser and go to `http://127.0.0.1:8050/`. You will see the NYC Apartment Analytics dashboard where you can switch between the Map and Treemap views.



### Exporting Data

To export the listings data to a CSV file, run the following command:
```
python tools/export_csv.py --out data/listings.csv
```
### Basic Visualizations

This project includes several scripts to generate interactive visualizations of the neighborhood data.


<iframe src="https://james-s-roche.github.io/html-demos/neighborhood_treemap.html" width="100%" height="400px" title="Embedded HTML Demo" sandbox="allow-scripts"></iframe>


-   **Neighborhood Treemaps:**
    ```
    python tools/neighborhood_diagram.py
    ```
    This will generate an HTML files `img/neighborhood_*.html` with a treemap, sunburst and icicle charts of the neighborhoods.
    
## Future Work

-   Add more data sources for listings.
-   Build a web interface for interacting with the data.
-   Develop machine learning models to predict apartment prices and identify good deals. See [kaggle](https://www.kaggle.com/datasets/harishkumardatalab/housing-price-prediction) for inspiration

## Legal/Ethical

This code is provided for educational purposes. Use responsibly and comply with website terms, `robots.txt`, and applicable laws. 

1) The api endpoint used is not listed in `robots.txt`, either allowed or disallowed. It seems likely that this is not intended to be scraped based on [this repo](https://github.com/purcelba/streeteasy_scrape) and the lack of a publicly documented API.

2) This information is scraped and sold already [here](https://rapidapi.com/realestator/api/streeteasy-api) which seems to have no affiliation with StreetEasy. I have no intention of profiting off of this data or sharing it widely. 
