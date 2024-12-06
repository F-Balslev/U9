## Install required modules with
``` pip install -r /path/to/requirements.txt ``` 

## Requires a ``` .env ``` file in the same folder as ``` main.py ``` that looks like

``` 
SQL_USERNAME=username
SQL_PASSWORD=password
```

to connect to a locally hosted MySQL server.

## Requires a ``` config.json ``` file in the same folder as ``` main.py ``` 
An example config file is available in the ``` config ``` branch.

## Run the script with
```python main.py```