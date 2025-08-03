from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    "owner": "datadevda1",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}

# Define the DAG
dag = DAG(
    "abc_liquor_inventory_pipeline",
    default_args=default_args,
    description="ABC Liquor Inventory Data Pipeline",
    catchup=False,
    tags=["abc", "liquor", "inventory", "scraping"],
    schedule="0 6 * * *"
)

# Define the working directory
WORKING_DIR = "/home/datadevda1/airflow/projects/abc_liquor_inventory/scripts"

# Task 1: Run scrape_pricing_site_fixed.py with Google Cloud credentials
scrape_pricing_data = BashOperator(
    task_id="scrape_pricing_data",
    bash_command=f'cd "{WORKING_DIR}" && export GOOGLE_APPLICATION_CREDENTIALS="{{{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}}}" && python3 scrape_pricing_site.py',
    dag=dag,
)

# Task 2: Run scrape_item_details_v2.py with Google Cloud credentials
scrape_item_details = BashOperator(
    task_id="scrape_item_details",
    bash_command=f'cd "{WORKING_DIR}" && export GOOGLE_APPLICATION_CREDENTIALS="{{{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}}}" && python3 scrape_item_details.py',
    dag=dag,
)

# Task 3: Run test_missing_image_urls_replace.py with all required credentials
replace_missing_images = BashOperator(
    task_id="replace_missing_images",
    bash_command=f"""cd "{WORKING_DIR}" && \
export GOOGLE_APPLICATION_CREDENTIALS="{{{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}}}" && \
export GOOGLE_API_KEY="{{{{ var.value.GOOGLE_API_KEY }}}}" && \
export GOOGLE_SEARCH_ENGINE_ID="{{{{ var.value.GOOGLE_SEARCH_ENGINE_ID }}}}" && \
python3 replace_missing_urls.py""",
    dag=dag,
)

# Task 4: Run test_replace_x_image_urls.py with all required credentials
replace_x_images = BashOperator(
    task_id="replace_x_images",
    bash_command=f"""cd "{WORKING_DIR}" && \
export GOOGLE_APPLICATION_CREDENTIALS="{{{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}}}" && \
export GOOGLE_SEARCH_ENGINE_ID="{{{{ var.value.GOOGLE_SEARCH_ENGINE_ID }}}}" && \
export GOOGLE_API_KEY="{{{{ var.value.GOOGLE_API_KEY }}}}" && \
python3 replace_x_image_urls.py""",
    dag=dag,
)

# Define task dependencies (sequential execution)
scrape_pricing_data >> scrape_item_details >> replace_missing_images >> replace_x_images
