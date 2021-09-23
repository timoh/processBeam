# processBeam

Apache Beam examples for reading and writing from Google BigQuery, PubSub and GCS.

Check the docstrings in 

1. pubspub.py
2. read_bigquery.py

..for more details.

## Important notes

Beam supports limited streaming sources and sinks, and they're not symmetrical.

Basically, you can read and write to/from PubSub in a streaming manner, but you can only write to BQ in a streaming manner; reading is batch-only as of 2021-09-23 at least for Python. Java might have some experimental batch reading of BQ.

So if you want to retain maximum flexibility for streaming, you should read and write to and from PubSub, and only do long-term persistence in BigQuery or GCS, that you can then only access in batch mode.