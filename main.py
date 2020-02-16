"""Data processing tool."""

import husker


def husker_worker(data, _):
    """Background Cloud Function to be triggered by Cloud Storage.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Storage.

    """
    blob_name = data['name']
    print(f'Blob: {blob_name}')
    prefix, storage_key, _ = blob_name.split('/')

    if prefix == 'web_scrape':
        if storage_key == 'planets_data_eu':
            husker.planets_data_eu(blob_name)
        elif storage_key == 'planets_data_nasa':
            husker.planets_data_nasa(blob_name)
    elif prefix == 'twitter_faves':
        husker.twitter_faves(blob_name)
