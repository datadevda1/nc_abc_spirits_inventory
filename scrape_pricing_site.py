# required imports
import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
from io import StringIO
from google.cloud import storage, bigquery

# scrape the ABC Quarterly Pricing List URL and return a list of divs that contain product information
def create_list_from_abc_request(url: str) -> list:
    """
    takes the NC ABC Quarterly Pricing List URL, pandas DataFrame column list as inputs and returns a pandas DataFrame after scraping the site for column data
    """
    # make the request and convert to text
    url_request = requests.get(url)
    html_text = url_request.text

    # parse the html text with BeautifulSoup
    soup = BeautifulSoup(html_text, "lxml")

    # find all div tags with class="container table-responsive"
    table_of_divs = soup.find_all("div", class_="container table-responsive")

    # exit script if there are more than one set of div tags in the tag iterable object
    if len(table_of_divs) > 1:
        print("Multiple divs found. Need to check the html file for the correct div.")
        # exit the program with an error code = 1
        exit(1)

    # convert the tag iterable object to a list
    product_listing_divs = table_of_divs[0]

    # return list of divs that contain product information
    return list(product_listing_divs)


# convert the list of divs that contain product information to a pandas DataFrame
def convert_abc_list_to_df(abc_list: list) -> pd.DataFrame:
    # create dictionary to store column values for DataFrame
    quarterly_pricing_list_column_names = {
        "nc_code": [],
        "supplier": [],
        "brand_name": [],
        "age": [],
        "proof": [],
        "size": [],
        "retail_price": [],
        "mxb_price": [],
        "product_category": [],
        "item_details_url": [],
    }

    # initialize the product category variable
    product_category = None

    # loop through each tag element to parse product data
    for tag_element in abc_list:
        # only include Tag objects and not NavigableString objects
        if isinstance(tag_element, Tag):
            h5_element = tag_element.find("h5")
            if h5_element:
                # scrapes product category (e.g. Boutique Collection - Bourbon) and continues to next element
                product_category = h5_element.text
                continue
            else:
                # check if the tag element has a class of row and font-weight-bold and if so, continue looping through the list
                if tag_element.get("class") == ["row", "font-weight-bold"]:
                    continue

                # check if the tag element has a class of list-generic and row and if so, parse the text and add respective elements to the column_names dictionary
                elif tag_element.get("class") == ["list-generic", "row"]:
                    # split the text into a list based on new lines
                    quarterly_pricing_list_values = tag_element.text.split("\n")

                    # remove the empty strings that appear at the beginning and end of the list
                    quarterly_pricing_list_values = quarterly_pricing_list_values[
                        1 : len(quarterly_pricing_list_values) - 1
                    ]

                    # clean up the onclick text that includes the ID part of the product URl
                    onclick_attr = tag_element.get("onclick")
                    if onclick_attr:
                        onclick_text = (
                            onclick_attr
                            .replace("window.location = '", "")
                            .replace("'", "")
                        )

                        # generate the full url for the item details
                        base_url = "https://abc2.nc.gov"
                        item_details_url = base_url + onclick_text

                        # parse out the values from the list and add them along with item details url to the respective columns in the quarterly_pricing_list_column_names dictionary
                        quarterly_pricing_list_column_names["nc_code"].append(
                            quarterly_pricing_list_values[0]
                        )
                        quarterly_pricing_list_column_names["supplier"].append(
                            quarterly_pricing_list_values[1]
                        )
                        quarterly_pricing_list_column_names["brand_name"].append(
                            quarterly_pricing_list_values[2]
                        )
                        quarterly_pricing_list_column_names["age"].append(
                            quarterly_pricing_list_values[3]
                        )
                        quarterly_pricing_list_column_names["proof"].append(
                            quarterly_pricing_list_values[4]
                        )
                        quarterly_pricing_list_column_names["size"].append(
                            quarterly_pricing_list_values[5]
                        )
                        quarterly_pricing_list_column_names["retail_price"].append(
                            quarterly_pricing_list_values[6]
                        )
                        quarterly_pricing_list_column_names["mxb_price"].append(
                            quarterly_pricing_list_values[7]
                        )
                        quarterly_pricing_list_column_names["product_category"].append(
                            product_category
                        )
                        quarterly_pricing_list_column_names["item_details_url"].append(
                            item_details_url
                        )

    # create DataFrame with the populated lists in the dictionary
    quarterly_pricing_list_df = pd.DataFrame(data=quarterly_pricing_list_column_names)
    return quarterly_pricing_list_df


# convert the pandas DataFrame to a csv and upload to Google Cloud Storage Bucket
def df_to_gcloud_bigquery(df: pd.DataFrame, bucket_name: str, blob_name: str) -> bool:
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
        return True
    except Exception as e:
        print(
            f"Could not upload data to Google Cloud Storage Bucket --> gs://{bucket_name}/{blob_name}\n"
        )
        print(f"Error: {e}")
        return False

# check if we can access a specific bucket
def check_bucket_access(bucket_name: str) -> bool:
    """Check if we can access a specific bucket"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Try to get bucket metadata
        bucket.reload()
        print(f"✅ Successfully accessed bucket: {bucket_name}")
        return True
    except Exception as e:
        print(f"❌ Cannot access bucket {bucket_name}: {e}")
        return False

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

if __name__ == "__main__":
    # Check bucket access first
    bucket_name = "dev-personal-projects"
    if check_bucket_access(bucket_name):
        # make request to ABC Pricing List URL and convert to list
        products_pricing_list = create_list_from_abc_request(
            url="https://abc2.nc.gov/Pricing/PriceList"
        )

        # convert list to DataFrame
        products_df = convert_abc_list_to_df(abc_list=products_pricing_list)

        # convert DataFrame to csv and push to Google Cloud Storage Bucket
        df_to_gcloud_bigquery(
            df=products_df,
            bucket_name=bucket_name,
            blob_name="nc-abc-bucket/abc_pricing_list.csv",
        )
        
        # load cloud storage csv into bigquery
        load_csv_to_bigquery(
            bucket_name=bucket_name,
            file_path="nc-abc-bucket/abc_pricing_list.csv",
            dataset_id="nc_abc",
            table_id="product_pricing_list",
        )
    else:
        print("Please fix bucket permissions or create the bucket first.") 
