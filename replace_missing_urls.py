import asyncio
import os
from typing import Any, Dict, List
import httpx
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from io import StringIO
from google.cloud import storage

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(30), wait=wait_exponential(multiplier=1, min=4, max=15))
async def google_custom_image_search(
    api_key: str,
    search_engine_id: str,
    query: str,
    nc_code: str,
    **params,
) -> str:
    """
    Applies the API key, Search Engine ID, query composed of {brand name and size},
    and params pre-filtered to image search only and outputs the JSON response
    from the Google Custom Search API
    """
    base_url = "https://www.googleapis.com/customsearch/v1"
    search_params = {
        "key": api_key,
        "cx": search_engine_id,
        "q": query,
        "searchType": "image",
        "fileType": "jpg,png",
    }

    try:
        # Use httpx.AsyncClient for async requests
        async with httpx.AsyncClient() as session:
            # Make the async request
            logger.info(f"Searching for {nc_code}~{query}...")
            response = await session.get(base_url, params=search_params)
            # Raise an error if the site cannot be reached
            response.raise_for_status()

            # Convert response to dictionary
            response_data = response.json()
            image_url = response_data.get("items", [])[0]["link"]
            logger.info(f"Found image URL for {nc_code}~{query}: {image_url}")
            return f"{nc_code}~{image_url}"
    except Exception as e:
        logger.error(f"Error finding image URL for {nc_code}~{query}: {e}")
        raise e

# upload the pandas DataFrame to a Google Cloud Storage Bucket
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

# convert BigQuery table to pandas DataFrame based on a given query
def bigquery_to_df(query: str) -> pd.DataFrame:
    """
    Convert BigQuery table to pandas DataFrame based on a given query
    """
    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    return df

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

async def process_urls(
    code_and_query_list: List[str], api_key: str, search_engine_id: str
) -> List[str]:
    """
    Process multiple requests to Google Custom Search API concurrently with the given code_and_query_list as input and return the results as a pandas DataFrame
    """

    async with httpx.AsyncClient() as session:
        tasks = [
            google_custom_image_search(
                api_key=api_key,
                search_engine_id=search_engine_id,
                nc_code=code_and_query.split("~")[0],
                query=code_and_query.split("~")[1],
            )
            for code_and_query in code_and_query_list
        ]
        results = await asyncio.gather(*tasks)

        return results


if __name__ == "__main__":
    # load environment variables
    load_dotenv("google_search.env")

    # read BigQuery table into pandas DataFrame
    query = """
    WITH products_all_details AS (
        SELECT
            ppl.nc_code,
            ppl.supplier,
            COALESCE(id.brand_name, ppl.brand_name) AS brand_name,
            ppl.age,
            ppl.proof,
            ppl.`size`,
            CAST(REGEXP_EXTRACT(ppl.`size`, r'^([0-9]*\.?[0-9]+)') AS FLOAT64) AS size_number_part,
            REGEXP_EXTRACT(ppl.`size`, r'([A-Za-z]+)$') AS size_string_part,
            ppl.retail_price,
            ppl.mxb_price,
            ppl.product_category,
            ppl.item_details_url,
            id.effective_date,
            id.bottles_per_case,
            id.case_cost_less_bailment,
            id.product_image_url
        FROM
            `agile-planet-462621-j7`.nc_abc.product_pricing_list ppl
        LEFT JOIN `agile-planet-462621-j7`.nc_abc.item_details id ON
            ppl.nc_code = id.nc_code
        WHERE
            1 = 1
                        ),
                        products_query_concat AS (
        SELECT
            pa.nc_code,
            pa.supplier,
            pa.brand_name,
            pa.age,
            pa.proof,
            pa.`size`,
            pa.size_number_part,
            pa.size_string_part,
            CONCAT(
                                pa.brand_name
                                , ' '
                                , CASE
                                    WHEN pa.size_number_part < 1
                                        THEN CAST(pa.size_number_part * CAST(1000 AS FLOAT64) AS FLOAT64)
                                        ELSE pa.size_number_part
                                  END
                                , CASE
                                    WHEN pa.size_number_part < 1
                                        THEN 'ml'
                                        ELSE 
                                            CASE
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
        FROM
            products_all_details pa
        WHERE
            1 = 1
                        )
        SELECT
            CONCAT(	
                                pqc.nc_code
                                , '~'
                                , pqc.query
            ) AS code_and_query
        FROM
            products_query_concat pqc
        WHERE
            1 = 1
            AND pqc.product_image_url IS NULL;
    """
    df = bigquery_to_df(query)
    code_and_query_list = df["code_and_query"].tolist()
    results = asyncio.run(
        process_urls(
            code_and_query_list,
            os.getenv("GOOGLE_API_KEY"),
            os.getenv("GOOGLE_SEARCH_ENGINE_ID"),
        )
    )

    # create lists for nc_code and image_url
    nc_code_list = [result.split("~")[0] for result in results]
    image_url_list = [result.split("~")[1] for result in results]

    # create DataFrame from nc_code_list and image_url_list
    df = pd.DataFrame({"nc_code": nc_code_list, "image_url": image_url_list})

    # convert DataFrame to csv and push to Google Cloud Storage Bucket
    df_to_gcloud_storage_bucket(
        df=df,
        bucket_name="dev-personal-projects",
        blob_name="nc-abc-bucket/image_url_backfill.csv",
    )

    # load csv to bigquery
    load_csv_to_bigquery(
        bucket_name="dev-personal-projects",
        file_path="nc-abc-bucket/image_url_backfill.csv",
        dataset_id="nc_abc",
        table_id="backfill_missing_image_urls",
    )
