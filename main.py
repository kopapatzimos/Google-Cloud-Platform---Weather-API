# imports
import requests
import pandas as pd
import settings
from locations import locations_dict
import os
from google.cloud import bigquery
import json

def upload_df_to_bigquery(dataframe: pd.DataFrame, project_id: str, dataset_id: str, table_name: str):
    """Uploads a pandas DataFrame to a BigQuery table.
    
    Args:
        dataframe (pd.DataFrame): The pandas DataFrame to upload.
        project_id (str): Your GCP project ID.
        dataset_id (str): The ID of the BigQuery dataset where the table will be created.
        table_name (str): The name of the BigQuery table to create.
    
    Returns:
        None
    """
    # Replace with the path to your downloaded JSON key file
    if settings.LOCAL_RUNNING:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"

    # Construct a BigQuery client object.
    client = bigquery.Client()
    dataset_id = f"{project_id}.{dataset_id}"
    
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west8" # Replace with your preferred location
    try:
       dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
       print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except:
        print("Dataset already exists")
    
    table_id = f"{dataset_id}.{table_name}"
    
    # Modify job_config for partitioning and truncating
    job_config = bigquery.LoadJobConfig(   
          autodetect=True,
          write_disposition= 'WRITE_TRUNCATE', #'WRITE_APPEND', 
          create_disposition='CREATE_IF_NEEDED'#,
          #range_partitioning = bigquery.RangePartitioning(
          #    field="id", # [Important!] Partition by location id to store only the latest forecast for each location  
          #    range_=bigquery.PartitionRange(interval=1),
        #)
    )
          
    print("Created a BigQuery job_config variable")
    
    # Make an API request to store the data into BigQuery
    try:
        job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.
        print("Saved data into BigQuery")
    except Exception as e:
        print(dataframe.dtypes)
        print(table_id)
        print(job_config)
        print(e)
        raise e


def fetch_api_data(url: str) -> dict:
		"""Fetches data from the specified API URL and returns the JSON response.
		
		Args:
		    url: The URL of the API endpoint.
		
		Returns:
		    The JSON data parsed from the API response, or raises an exception on error.
		
		Raises:
		    requests.exceptions.HTTPError: If the API request fails.
		"""
		
		response = requests.get(url)
		response.raise_for_status()  # Raise exception for non-200 status codes
		
		return response.json()


def get_weather_data(locations: dict,  api_key: str):
    '''
    Fetches data for spesific location. Returns json response
    Args:
        location : The dictionary with the metrics of location
        api_key: The API access token to use Weather API

    Returns:
        The json data that retrieve from weather api
    '''

    base_url = "https://api.openweathermap.org/data/2.5/"
    weather_data = {"current": {}, "forecast": {}}
    for location_name, location in locations.items():
        try:
            current_url = f"{base_url}weather?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"
            forecast_url = f"{base_url}forecast?lat={location['lat']}&lon={location['lon']}&appid={api_key}&units=metric"
            
            # Fetch and store data using a single function call
            weather_data["current"][location_name] = fetch_api_data(current_url)
            weather_data["forecast"][location_name] = fetch_api_data(forecast_url)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for location {location_name}: {e}")
    
    print('Weather Data for all locations have been fetched...')
    return weather_data


def transform_current_weather_data(data_dict: dict) -> pd.DataFrame:
    """Transforms weather API data into a Pandas DataFrame suitable for BigQuery.

    Args:
        data_dict (dict): A dictionary containing weather data.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the transformed weather data.
    """

    # Preprocess the value of 'weather' key (in some cases the API returns a list instead of a dictionary)
    # if isinstance(data_dict['weather'], list):  # Check if 'weather' is a list and select the first element
    #     data_dict['weather'] = data_dict['weather'][0]

    # Flatten data structure
    flattened_data = {}
    for key, value in data_dict.items():
        if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flattened_data[f"{key}_{sub_key}"] = sub_value
        else:
            flattened_data[key] = value

    # Convert DataFrame and handle datetime if necessary
    data_df = pd.DataFrame([flattened_data])
    if 'dt' in data_df.columns:
        data_df['dt_txt'] = pd.to_datetime(data_df['dt'], unit='s')
        data_df['dt_txt'] = data_df['dt_txt'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return data_df


def convert_weather_api_dict_to_dataframe(data_dict: dict) -> pd.DataFrame:
    """
    Converts a nested dictionary containing weather data from the Weather API to a Pandas DataFrame.

    Args:
        data_dict (dict): The dictionary containing the weather data.

    Returns:
        pd.DataFrame: A DataFrame representing the weather data.
    """

    extracted_data = {}
    for key, value in data_dict.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                extracted_data[f"{key}_{sub_key}"] = sub_value
        else:
            extracted_data[key] = value

    return pd.DataFrame([extracted_data])


def transform_forecasted_weather_data(data_dict: dict) -> pd.DataFrame:
    """
    Transforms the forecasted weather data from the Weather API into a Pandas DataFrame.

    Args:
        data_dict (dict): The dictionary containing the forecasted weather data.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed forecasted weather data.
    """

    city_dict = data_dict['city']
    city_df = convert_weather_api_dict_to_dataframe(city_dict)

    forecasts_dict = data_dict['list']
    forecast_df = pd.DataFrame()
    for forecast_item in forecasts_dict:
        forecast_item['weather'] = forecast_item['weather'][0]
        forecast_item_df = convert_weather_api_dict_to_dataframe(forecast_item)
        forecast_df = pd.concat([forecast_df, forecast_item_df], ignore_index=True)

    # Merge forecast_df with city_df into a single DataFrame and return the result
    # Since city_df has only one row, we use 'cross' join type to combine each row from forecast_df with the single row from city_df.
    return forecast_df.merge(city_df, how='cross')


def main(request: dict) -> str:
    try:
        request_body = request.get_json()
    except:
        request_body = json.loads(request)
    
    locations = locations_dict
    data = get_weather_data(locations, api_key=settings.API_KEY)
    current_weather = pd.DataFrame()
    for key, value in data['current'].items():
        current_weather = pd.concat([current_weather, transform_current_weather_data(value)])
    forecast_weather = pd.DataFrame()
    for key, value in data['forecast'].items():
        forecast_weather = pd.concat([forecast_weather, transform_forecasted_weather_data(value)])

    upload_df_to_bigquery(dataframe=current_weather, project_id=settings.GCP_PROJECT_ID, dataset_id=settings.DATASET_ID, table_name = 'current_weather')
    upload_df_to_bigquery(dataframe=forecast_weather, project_id=settings.GCP_PROJECT_ID, dataset_id=settings.DATASET_ID, table_name = 'forecasted_weather')

    return '200, Success'

if __name__ == '__main__':
    data = {} # This is used as the request body
    payload = json.dumps(data)
    print(main(payload))

