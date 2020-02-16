"""Functions used to process data for DOTUFP projects.

All data processing for DOTUFP is done through these functions.
"""

from google.cloud import storage
from google.cloud import kms_v1
from dateutil.parser import parse
import pandas as pd
import redis
import json
import io


def _download_blob_as_str(blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket('dotufp-raw')
    blob = bucket.blob(blob_name)
    text = blob.download_as_string()

    return text


def _upload_data(bucket_name: str, blob_name: str, data: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(data)


def _update_redis(key: str, value: str):
    secrets = _get_secrets()

    r = redis.Redis(host=secrets['redis']['host'],
                    port=secrets['redis']['port'],
                    password=secrets['redis']['password'])

    current_value = str(r.get(key))

    if int(current_value) <= int(value) <= 1.1 * int(current_value):
        r.set(key, value)
        print(f'{key} is now {value}')
    else:
        raise ValueError(f'new {key} is maybe {value}, current value is {current_value}')


def _get_secrets():
    """Fetch and decrypt project secrets."""
    # get encrypted secrets
    storage_client = storage.Client()
    bucket = storage_client.bucket('dotufp-sm')
    blob = bucket.blob('vaqmr-secrets.v3.json.encrypted')
    ciphertext = blob.download_as_string()

    # decrypt secrets
    kms_client = kms_v1.KeyManagementServiceClient()
    key_name = kms_client.crypto_key_path('secret-manager-258521', 'global', 'dotufp-secrets', 'dotufp-secrets-key')
    secrets = kms_client.decrypt(key_name, ciphertext)

    return json.loads(secrets.plaintext)


def twitter_faves(blob_name: str):
    """Process raw response data from the catalog at exoplanet.eu."""
    prefix, storage_key, file_name = blob_name.split('/')
    collected_timestamp = file_name.split('.')[0]

    raw_str = _download_blob_as_str(blob_name)

    tweets = json.loads(raw_str)

    for tweet in tweets:
        output_data = {'husker_processor': 'twitter_faves',
                       'husker_data_version': '0',
                       'data_source': 'Twitter API',
                       'collected_timestamp': collected_timestamp,
                       'text': tweet.get('full_text') or tweet.get('text'),
                       'id': tweet['id'],
                       'created_at': parse(tweet['created_at']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                       'user_id': tweet['user']['id'],
                       'user_screen_name': tweet['user']['screen_name'],
                       'user_followers_count': tweet['user']['followers_count'],
                       'retweet_count': tweet['retweet_count'],
                       'favorite_count': tweet['favorite_count']}

        output_blob_name = f'{prefix}/{storage_key}/{tweet["id"]}.{collected_timestamp}.json'

        _upload_data('dotufp-data', output_blob_name, json.dumps(output_data))


def planets_data_eu(blob_name: str):
    """Process raw response data from the catalog at exoplanet.eu."""
    _, _, file_name = blob_name.split('/')
    collected_timestamp = file_name.split('.')[0]

    raw_str = _download_blob_as_str(blob_name)

    planets_data = pd.read_csv(io.BytesIO(raw_str))
    num_planets = planets_data.shape[0]

    output_data = {'husker_processor': 'planets_data_eu',
                   'husker_data_version': '1',
                   'collected_timestamp': collected_timestamp,
                   'data_source': 'The ExtrasolarPlanets Encyclopaedia',
                   'num_planets': num_planets}

    output_blob_name = blob_name.replace('.raw', '.json')

    _upload_data('dotufp-data', output_blob_name, json.dumps(output_data))
    _update_redis('planets_data_eu', json.dumps(num_planets))


def planets_data_nasa(blob_name: str):
    """Process raw response data from the NASA exoplanet archive."""
    _, _, file_name = blob_name.split('/')
    collected_timestamp = file_name.split('.')[0]

    raw_str = _download_blob_as_str(blob_name)

    planets_data = pd.read_csv(io.BytesIO(raw_str))
    num_planets = planets_data.shape[0]

    output_data = {'husker_processor': 'planets_data_nasa',
                   'husker_data_version': '1',
                   'collected_timestamp': collected_timestamp,
                   'data_source': 'NASA Exoplanet Archive',
                   'num_planets': num_planets}

    output_blob_name = blob_name.replace('.raw', '.json')

    _upload_data('dotufp-data', output_blob_name, json.dumps(output_data))
    _update_redis('planets_data_nasa', json.dumps(num_planets))
