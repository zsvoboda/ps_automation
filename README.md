# Pure Storage FlashArray Automation Scripts
This repository contains a collection of scripts that can be used to automate various tasks on a Pure Storage FlashArray.  
The scripts are written in Python and leverage the [Pure Storage Python REST Client](https://pypi.org/project/py-pure-client/).

## Installation
The scripts require the Pure Storage Python REST Client to be installed.  The easiest way to install the client is to use pip.  
The following command will install the client and all of its dependencies:

```shell
pip install -r ./requirements.txt
```

## Environment Variables
The scripts require the following environment variables to be set:

```shell
export FA_API_TOKEN="1be2ba07-7567-1ac3-1274-5e565d23b74f"
export FA_ARRAY_HOST="10.0.0.31"
```

## Tests
To execute tests, run the following command:

```shell
PYTHONPATH=./src             
./tests/flash_array/test_flash_array.py
```

## Pure Storage REST API Examples
The [test_flash_array.py](./tests/flash_array/test_flash_array.py) script contains examples of 
how to use the Pure Storage REST API to perform various tasks on a FlashArray.