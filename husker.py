"""Functions used to process data for DOTUFP projects.

All data processing for DOTUFP is done through these functions.
"""

from google.cloud import storage
import pandas as pd
import json
import io

def _download_blob_as_str(blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket('dotufp-raw')
    blob = bucket.blob(blob_name)
    ciphertext = blob.download_as_string()

def _upload_data(bucket_name: str, blob_name: str, data: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(data)


def planets_data_eu(blob_name: str):
    """Process raw response data from the catalog at exoplanet.eu."""
    raw_str = _download_blob_as_str(blob_name)

    planets_data = pd.read_csv(io.BytesIO(raw_str))
    num_planets = planets_data.shape[0]

    output_data = {'husker_processor': 'planets_data_eu',
                             'husker_data_version': '0.1.0',
                             'data_source': 'The ExtrasolarPlanets Encyclopaedia',
                             'num_planets': num_planets}

    output_blob_name = blob_name.replace('.raw', '.json')

    _upload_data('dotufp-data', output_blob_name, json.dumps(output_data))

def planets_data_nasa(blob_name: str):
    """Process raw response data from the NASA exoplanet archive."""
    raw_str = _download_blob_as_str(blob_name)

    planets_data = pd.read_csv(io.BytesIO(raw_str))
    num_planets = planets_data.shape[0]

    output_data = {'husker_processor': 'planets_data_nasa',
                             'husker_data_version': '0.1.0',
                             'data_source': 'NASA Exoplanet Archive',
                             'num_planets': num_planets}

    output_blob_name = blob_name.replace('.raw', '.json')

    _upload_data('dotufp-data', output_blob_name, json.dumps(output_data))
