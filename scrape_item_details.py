# required imports
# from debugpy.server.api import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup, Tag
import pandas as pd
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import sys
import lxml
from google.cloud import bigquery
from io import StringIO
from google.cloud import storage

# Enable debugpy
# debugpy.listen(("0.0.0.0", 5678))
# print("Waiting for debugger attach...")
# debugpy.wait_for_client()

# Configure logging for printing to console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def scrape_item_details(session: aiohttp.ClientSession, url: str) -> dict:
    """
    Scrape item details from the ABC website asynchronously with the given url as input and returns a dictionary with the item details
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
        async with session.get(url, ssl=False) as response:
            # print message to show which url is being scraped
            logger.info(f"Scraping {url}")
            if response.status != 200:
                logger.error(f"Failed to fetch {url}: Status {response.status}")
                # exit with error code 1
                sys.exit(1)

            # Parse the HTML content using lxml
            html_content = await response.text()
            soup = BeautifulSoup(html_content, "lxml")

            # Find all div tags with class="container" style="min-height:70vh;"
            item_details_div = soup.find_all(
                "div", class_="container", style="min-height:70vh;"
            )

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
                        item_details_column_names["effective_date"] = h3_tag_text_list[
                            1
                        ]

                    elif element.get("class") == ["row"]:
                        # Get text from row divs
                        div_tag_text = element.text.strip()
                        new_line_div_tag_text = [
                            item for item in div_tag_text.split("\n") if item != ""
                        ]
                        # Check if the length of the list is 4 or 2
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
                    item_details_column_names["product_image_url"] = (
                        full_product_image_url
                    )

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
                item_details_column_names["bottles_per_case"] = (
                    item_details_column_values["Bottles per Case"]
                )

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


async def process_urls(urls: List[str]) -> List[dict]:
    """
    Process multiple URLs concurrently with the given urls as input and return the scraped data as a list of dictionaries
    """
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_item_details(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results


# convert BigQuery table to pandas DataFrame based on a given query
def bigquery_to_df(query: str) -> pd.DataFrame:
    """
    Convert BigQuery table to pandas DataFrame based on a given query
    """
    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    return df

def df_to_gcloud_storage_bucket(
    df: pd.DataFrame, bucket_name: str, blob_name: str
) -> bool:
    try:
        # create string buffer
        buffer = StringIO()

        # Save DataFrame to CSV in the buffer
        df.to_csv(buffer, index=False)

        # reset cursor to beginning of the CSV
        buffer.seek(0)

        # Get the CSV content as a string
        csv_content = buffer.getvalue()

        # Initialize Google Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Upload the CSV content
        blob.upload_from_string(csv_content, content_type="text/csv")
        print(
            f"{df.shape[0]} rows of data uploaded to Google Cloud Storage Bucket --> gs://{bucket_name}/{blob_name}"
        )
    except Exception as e:
        print(
            f"Could not upload data to Google Cloud Storage Bucket --> gs://{bucket_name}/{blob_name}\n"
        )
        print(f"Error: {e}")

def load_csv_to_bigquery(bucket_name, file_path, dataset_id, table_id):
    """Loads a CSV file from Cloud Storage into a BigQuery table.

    Args:
        bucket_name: The name of the Cloud Storage bucket.
        file_path: The path to the CSV file in the bucket (e.g., 'data/my_file.csv').
        dataset_id: The ID of the BigQuery dataset.
        table_id: The ID of the BigQuery table.
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Construct the full table ID.
    table_ref = client.dataset(dataset_id).table(table_id)

    # Configure the load job.
    job_config = bigquery.LoadJobConfig(
        # schema=[
        # # Define the schema here (if you don't have a header row). If you have a header row,
        # # you can comment out this 'schema' section and set 'skip_leading_rows' to 1.
        # # Example schema:
        # # bigquery.SchemaField("column1", "STRING"),
        # # bigquery.SchemaField("column2", "INTEGER"),
        # # bigquery.SchemaField("column3", "FLOAT"),
        # ],
        skip_leading_rows=1,  # Skip header row if it exists in the CSV
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if the table exists
    )

    uri = f"gs://{bucket_name}/{file_path}"

    # Load data from GCS into BigQuery.
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_ref)  # Make an API request.
    print(
        "Loaded {} rows into {}:{}".format(
            destination_table.num_rows, dataset_id, table_id
        )
    )

async def main():
    # read in the urls from the BigQuery table and convert to pandas DataFrame
    query = """
    SELECT
        item_details_url
    FROM
        `agile-planet-462621-j7`.nc_abc.product_pricing_list ppl 
    """
    df = bigquery_to_df(query)
    urls = df["item_details_url"].tolist()
    results = await process_urls(urls)

    # Initialize lists to store data
    brand_names = []
    effective_dates = []
    nc_codes = []
    bottle_sizes = []
    proofs = []
    bottles_per_cases = []
    upcs = []
    retail_prices = []
    mxb_prices = []
    case_costs = []
    distillers = []
    product_image_urls = []

    # Process results
    for result in results:
        brand_names.append(result["brand_name"])
        effective_dates.append(result["effective_date"])
        nc_codes.append(result["nc_code"])
        bottle_sizes.append(result["bottle_size"])
        proofs.append(result["proof"])
        bottles_per_cases.append(result["bottles_per_case"])
        upcs.append(result["upc"])
        retail_prices.append(result["retail_price"])
        mxb_prices.append(result["mxb_price"])
        case_costs.append(result["case_cost_less_bailment"])
        distillers.append(result["distiller"])
        product_image_urls.append(result["product_image_url"])

    # Create DataFrame from lists
    df = pd.DataFrame(
        {
            "nc_code": nc_codes,
            "brand_name": brand_names,
            "effective_date": effective_dates,
            "bottle_size": bottle_sizes,
            "proof": proofs,
            "bottles_per_case": bottles_per_cases,
            "upc": upcs,
            "retail_price": retail_prices,
            "mxb_price": mxb_prices,
            "case_cost_less_bailment": case_costs,
            "distiller": distillers,
            "product_image_url": product_image_urls,
        }
    )

    # Save CSV buffer to Google Cloud Storage Bucket
    item_details_file_name = "individual_item_details.csv"
    df_to_gcloud_storage_bucket(
        df=df,
        bucket_name="dev-personal-projects",
        blob_name="nc-abc-bucket/individual_item_details.csv",
    )

    # Load CSV from Google Cloud Storage Bucket to BigQuery
    load_csv_to_bigquery(
        bucket_name="dev-personal-projects",
        file_path="nc-abc-bucket/individual_item_details.csv",
        dataset_id="nc_abc",
        table_id="item_details",
    )


if __name__ == "__main__":
    print("Starting scrape_item_details_v2.py script...")
    try:
        print("Initializing asyncio...")
        asyncio.run(main())
        print("Script completed successfully!")
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
