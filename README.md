# Husker

a data processing tool

## Adding a new processor

Add data processing function in husker.py that takes a blob name as input.

Add function to routing tree in main.husker_worker().

Test locally:

```python
import main

blob_name = 'twitter_faves/dotufp/2019-11-17T05:35:26.raw'
main.husker_work(data={'name': blob_name}, context='context')
```

- check output in gs://dotufp-data/

- deploy husker
