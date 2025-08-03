from dotenv import load_dotenv
import os
import asyncio
import pandas as pd
# import debugpy
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import bigquery
import httpx
from bs4 import BeautifulSoup
from typing import List
import sys
from bs4 import Tag
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# enable debugpy
# debugpy.listen(("0.0.0.0", 5678))
# print("Waiting for debugger to attach...")
# debugpy.wait_for_client()

# convert BigQuery table to pandas DataFrame based on a given query
def bigquery_to_df(query: str) -> pd.DataFrame:
    """
    Convert BigQuery table to pandas DataFrame based on a given query
    """
    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    return df

# retry 3 times if the request fails
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
# scrape the item details from the ABC website, return a dictionary with the item details
async def scrape_item_details(client: httpx.Client, url: str) -> dict:
    """
    Scrape item details from the ABC website with the given url as input and returns a dictionary with the item details
    """
    # create a dictionary to store the data for each column
    item_details_column_names = {
        "nc_code": "",
        "brand_name": "",
        "effective_date": "",
        "bottle_size": "",
        "proof": "",
        "bottles_per_case": "",
        "upc": "",
        "retail_price": "",
        "mxb_price": "",
        "case_cost_less_bailment": "",
        "distiller": "",
        "product_image_url": "",
    }

    try:
        # Make the request using httpx
        response = client.get(url)
        logger.info(f"Scraping {url}")

        if response.status_code != 200:
            logger.error(f"Failed to fetch {url}: Status {response.status_code}")
            # exit with error code 1
            sys.exit(1)

        # Parse the HTML content using lxml
        html_content = response.text
        soup = BeautifulSoup(html_content, "lxml")

        # Find all div tags with class="container" style="min-height:70vh;"
        item_details_div = soup.find_all(
            "div", class_="container", style="min-height:70vh;"
        )

        # Check if the item details div is found
        if not item_details_div:
            logger.warning(f"No item details div found for {url}")
            return item_details_column_names

        # Get the product listing divs
        product_listing_divs = item_details_div[0]

        # Create an empty dictionary to store the column values
        item_details_column_values = {}

        # Loop through the div tags
        for element in product_listing_divs.find_all(recursive=False):
            if isinstance(element, Tag):
                if element.find("h3", class_="mt-5 pt-3"):
                    # Get brand name and effective date
                    h3_tag_text = element.text.strip()
                    h3_tag_text_list = h3_tag_text.split(" Effective Date: ")
                    item_details_column_names["brand_name"] = h3_tag_text_list[0]
                    item_details_column_names["effective_date"] = h3_tag_text_list[1]

                elif element.get("class") == ["row"]:
                    # Get text from row divs
                    div_tag_text = element.text.strip()
                    new_line_div_tag_text = [
                        item for item in div_tag_text.split("\n") if item != ""
                    ]
                    # Check if the length of the list is 4 or 2 (4 is for NC Code, Brand Name, Effective Date, and Product Image URL, 2 is for NC Code and Brand Name)
                    if len(new_line_div_tag_text) == 4:
                        desc_1 = new_line_div_tag_text[0].replace(":", "")
                        value_1 = new_line_div_tag_text[1]
                        desc_2 = new_line_div_tag_text[2].replace(":", "")
                        value_2 = new_line_div_tag_text[3]

                        item_details_column_values[desc_1] = value_1
                        item_details_column_values[desc_2] = value_2

                    elif len(new_line_div_tag_text) == 2:
                        desc_1 = new_line_div_tag_text[0].replace(":", "")
                        value_1 = new_line_div_tag_text[1]
                        item_details_column_values[desc_1] = value_1

        # Find the product image
        img_div = soup.find("div", class_="row", style="padding:15px;")
        if img_div and img_div.find("img"):
            img_tag = img_div.find("img")
            if img_tag and "src" in img_tag.attrs:
                base_url = "https://abc2.nc.gov"
                product_image_url = img_tag["src"]
                full_product_image_url = base_url + product_image_url
                item_details_column_names["product_image_url"] = full_product_image_url

        # Clean up and assign values to the dictionary
        if "NC Code" in item_details_column_values:
            nc_code = item_details_column_values["NC Code"]
            nc_code = nc_code[:2] + "-" + nc_code[2:]
            item_details_column_names["nc_code"] = nc_code

        if "Bottle Size" in item_details_column_values:
            item_details_column_names["bottle_size"] = item_details_column_values[
                "Bottle Size"
            ]

        if "Proof" in item_details_column_values:
            item_details_column_names["proof"] = item_details_column_values["Proof"]

        if "Bottles per Case" in item_details_column_values:
            item_details_column_names["bottles_per_case"] = item_details_column_values[
                "Bottles per Case"
            ]

        if "UPC" in item_details_column_values:
            item_details_column_names["upc"] = item_details_column_values["UPC"]

        if "Retail Price" in item_details_column_values:
            item_details_column_names["retail_price"] = item_details_column_values[
                "Retail Price"
            ]

        if "Mixed Beverage Price" in item_details_column_values:
            item_details_column_names["mxb_price"] = item_details_column_values[
                "Mixed Beverage Price"
            ]

        if "Case Cost Less Bailment" in item_details_column_values:
            item_details_column_names["case_cost_less_bailment"] = (
                item_details_column_values["Case Cost Less Bailment"]
            )

        if "Distiller" in item_details_column_values:
            item_details_column_names["distiller"] = item_details_column_values[
                "Distiller"
            ]

        return item_details_column_names

    except Exception as e:
        logger.error(f"Error scraping {url}: {str(e)}")
        # exit with error code 1
        sys.exit(1)

# process multiple URLs with the given urls as input and return the scraped data as a list of dictionaries
async def process_urls(urls: List[str]) -> List[dict]:
    """
    Process multiple URLs with the given urls as input and return the scraped data as a list of dictionaries
    """
    with httpx.Client(timeout=30.0) as client:
        results = []
        for url in urls:
            result = await scrape_item_details(client, url)
            results.append(result)
        return results

# go here

# @retry(stop=stop_after_attempt(30), wait=wait_exponential(multiplier=1, min=4, max=15))
# async def google_custom_image_search(
#     api_key: str,
#     search_engine_id: str,
#     query: str,
#     nc_code: str,
#     **params,
# ) -> str:
#     """
#     Applies the API key, Search Engine ID, query composed of {brand name and size},
#     and params pre-filtered to image search only and outputs the JSON response
#     from the Google Custom Search API
#     """
#     base_url = "https://www.googleapis.com/customsearch/v1"
#     search_params = {
#         "key": api_key,
#         "cx": search_engine_id,
#         "q": query,
#         "searchType": "image",
#         "fileType": "jpg,png",
#     }

#     try:
#         # Use httpx.AsyncClient for async requests
#         async with httpx.AsyncClient() as session:
#             # Make the async request
#             logger.info(f"Searching for {nc_code}~{query}...")
#             response = await session.get(base_url, params=search_params)
#             # Raise an error if the site cannot be reached
#             response.raise_for_status()

#             # Convert response to dictionary
#             response_data = response.json()
#             image_url = response_data.get("items", [])[0]["link"]
#             logger.info(f"Found image URL for {nc_code}~{query}: {image_url}")
#             return f"{nc_code}~{image_url}"
#     except Exception as e:
#         logger.error(f"Error finding image URL for {nc_code}~{query}: {e}")
#         raise e

# get the nc_code and image_url in which the image_url starts with "x-raw-image" from the bigquery table
query = """
WITH products_all_details AS (
    SELECT ppl.nc_code,
        ppl.supplier,
        COALESCE(id.brand_name, ppl.brand_name) AS brand_name,
        ppl.age,
        ppl.proof,
        ppl.`size`,
        CAST(
            REGEXP_EXTRACT(ppl.`size`, r'^([0-9]*\.?[0-9]+)') AS FLOAT64
        ) AS size_number_part,
        REGEXP_EXTRACT(ppl.`size`, r'([A-Za-z]+)$') AS size_string_part,
        ppl.retail_price,
        ppl.mxb_price,
        ppl.product_category,
        ppl.item_details_url,
        id.effective_date,
        id.bottles_per_case,
        id.case_cost_less_bailment,
        id.product_image_url
    FROM `agile-planet-462621-j7`.nc_abc.product_pricing_list ppl
        LEFT JOIN `agile-planet-462621-j7`.nc_abc.item_details id ON ppl.nc_code = id.nc_code
    WHERE 1 = 1
),
products_query_concat AS (
    SELECT pa.nc_code,
        pa.supplier,
        pa.brand_name,
        pa.age,
        pa.proof,
        pa.`size`,
        pa.size_number_part,
        pa.size_string_part,
        CONCAT(
            pa.brand_name,
            ' ',
            CASE
                WHEN pa.size_number_part < 1 THEN CAST(
                    pa.size_number_part * CAST(1000 AS FLOAT64) AS FLOAT64
                )
                ELSE pa.size_number_part
            END,
            CASE
                WHEN pa.size_number_part < 1 THEN 'ml'
                ELSE CASE
                    WHEN lower(pa.size_string_part) = 'ml' THEN 'ml'
                    WHEN lower(pa.size_string_part) = 'l' THEN 'L'
                END
            END
        ) AS query,
        pa.retail_price,
        pa.mxb_price,
        pa.product_category,
        pa.item_details_url,
        pa.effective_date,
        pa.bottles_per_case,
        pa.case_cost_less_bailment,
        pa.product_image_url
    FROM products_all_details pa
    WHERE 1 = 1
)
SELECT 
	pqc.nc_code,
	pqc.item_details_url,	
    CONCAT(
            pqc.nc_code,
            '~',
            REGEXP_REPLACE(pqc.query, r'\d+[yY]', ''),
             ' ',
            CASE 
                WHEN LOWER(pqc.product_category) LIKE '%--%' THEN SPLIT(pqc.product_category, '--')[SAFE_OFFSET(0)]
                ELSE pqc.product_category
            END
    ) AS code_and_query
FROM products_query_concat pqc
INNER JOIN `agile-planet-462621-j7`.nc_abc.backfill_missing_image_urls ppl ON 1 = 1
	AND ppl.nc_code = pqc.nc_code
	AND LOWER(ppl.image_url) LIKE '%x-raw-image%'
WHERE 1 = 1
"""

df = bigquery_to_df(query)

if __name__ == "__main__":
    # load environment variables
    load_dotenv("google_search.env")

    no_x_raw_image_urls = False
    retry_count = 0

    # define function that searches for new image urls to replace the x-raw-image urls if found
    def replace_x_raw_image_urls():
        # Get the item_details_url from the dataframe instead of using code_and_query
        # The code_and_query contains search queries, not URLs
        item_details_urls = df["item_details_url"].tolist()
        nc_codes = df["nc_code"].tolist()
        
        # Filter out None or empty URLs
        valid_urls = []
        valid_nc_codes = []
        for url, nc_code in zip(item_details_urls, nc_codes):
            if url and url.strip():
                valid_urls.append(url)
                valid_nc_codes.append(nc_code)
        
        if not valid_urls:
            print("No valid URLs found to scrape")
            return [], []
        
        print(f"Scraping {len(valid_urls)} URLs for image data...")
        results = asyncio.run(process_urls(valid_urls))
        
        # Extract nc_code and product_image_url from the scraped results
        nc_code_list = []
        image_url_list = []
        
        for i, result in enumerate(results):
            if isinstance(result, dict):
                nc_code_list.append(result.get("nc_code", ""))
                image_url_list.append(result.get("product_image_url", ""))
            else:
                # Fallback if result is not a dictionary
                nc_code_list.append("")
                image_url_list.append("")

        return nc_code_list, image_url_list

    while not no_x_raw_image_urls:

        try:
            nc_code_list, image_url_list = replace_x_raw_image_urls()
            # code_and_query_list = df["code_and_query"].tolist()
            # results = asyncio.run(process_urls(code_and_query_list, os.getenv("GOOGLE_API_KEY"), os.getenv("GOOGLE_SEARCH_ENGINE_ID")))
            
            # # create lists for nc_code and image_url
            # nc_code_list = [result.split("~")[0] for result in results]
            # image_url_list = [result.split("~")[1] for result in results]

            # # if no new image urls found, exit the script so that the update query is not executed
            # if len(nc_code_list) == 0:
            #     print("No new image urls found, exiting...")
            #     exit(1)
            
            # check if the image_url_list contains any "x-raw-image:"
            if any("x-raw-image:" in image_url for image_url in image_url_list):
                print("x-raw-image: found in the image_url_list, searching for new image urls...")
                nc_code_list, image_url_list = replace_x_raw_image_urls()

            update_query = """
            UPDATE `agile-planet-462621-j7`.nc_abc.backfill_missing_image_urls
                SET image_url = '{image_url}'
            WHERE nc_code = '{nc_code}'
            """

            # create a bigquery client, loop through the nc_code_list and image_url_list and execute the update_query
            client = bigquery.Client()
            for nc_code, image_url in zip(nc_code_list, image_url_list):
                update_query = update_query.replace("{image_url}", image_url).replace("{nc_code}", nc_code)
                query_job = client.query(update_query)
                query_job.result()  # This commits the changes
                print(f"Query completed. Affected rows: {query_job.num_dml_affected_rows}")
            
            print("All updates to nc_abc.backfill_missing_image_urls table completed")

            no_x_raw_image_urls = True

        except Exception as e:
            print(f"Error: {e}")
            exit()


    # run the ETL process to update the final_product_table table
    etl_query = """
        TRUNCATE TABLE `agile-planet-462621-j7`.nc_abc.final_product_list;
        INSERT INTO `agile-planet-462621-j7`.nc_abc.final_product_list (
            nc_code,
            supplier,
            brand_name,
            age,
            proof,
            `size`,
            retail_price,
            mxb_price,
            product_category,
            bottles_per_case,
            upc,
            case_cost_less_bailment,
            item_details_url,
            product_image_url,
            size_with_proper_units,
            effective_date
        )
        WITH product_list AS (
            SELECT
                ppl.nc_code,
                COALESCE(id.distiller, ppl.supplier) as supplier,
                id.brand_name,
                ppl.age,
                ppl.proof,
                ppl.`size`,
                CAST (REGEXP_EXTRACT(size, r'^([0-9]*\.?[0-9]+)') AS FLOAT64) AS size_numeric,
                REGEXP_EXTRACT(size, r'[a-zA-Z]+$') AS size_unit,
                ppl.retail_price,
                ppl.mxb_price,
                ppl.product_category,
                id.bottles_per_case,
                id.upc,
                id.case_cost_less_bailment,
                ppl.item_details_url,
                COALESCE(id.product_image_url, bmi.image_url) AS product_image_url,
                id.effective_date
            FROM
                `agile-planet-462621-j7`.nc_abc.product_pricing_list ppl
            LEFT JOIN `agile-planet-462621-j7`.nc_abc.item_details id ON
                id.nc_code = ppl.nc_code
            LEFT JOIN `agile-planet-462621-j7`.nc_abc.backfill_missing_image_urls bmi ON
                bmi.nc_code = ppl.nc_code
            WHERE 
                1 = 1
            )
        SELECT
            pl.nc_code,
            pl.supplier,
            pl.brand_name,
            pl.age,
            pl.proof,
            pl.`size`,
            --pl.size_numeric,
            --pl.size_unit,
            pl.retail_price,
            pl.mxb_price,
            pl.product_category,
            pl.bottles_per_case,
            pl.upc,
            pl.case_cost_less_bailment,
            pl.item_details_url,
            pl.product_image_url,
            CONCAT(
                CASE 
                        WHEN (pl.size_numeric < 1 AND pl.size_unit = 'L')
                                THEN CAST(pl.size_numeric * CAST(1000 AS FLOAT64) AS FLOAT64)
                                ELSE pl.size_numeric
                END,
                CASE 
                        WHEN (pl.size_numeric > 1 AND UPPER(pl.size_unit) = 'ML') OR (pl.size_numeric < 1 AND pl.size_unit = 'L')
                                THEN 'ml'
                                ELSE pl.size_unit
                END
            ) AS size_with_proper_units,
            pl.effective_date
        FROM product_list pl
        WHERE 
            1 = 1
   """ 

    # execute the etl_query
    client = bigquery.Client()
    query_job = client.query(etl_query)
    query_job.result()  # This commits the changes

    print("ETL process completed for nc_abc.final_product_list table")