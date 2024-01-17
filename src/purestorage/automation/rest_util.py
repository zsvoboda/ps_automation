class FlashArrayError(Exception):

    def __init__(self, api_response=None):
        super(FlashArrayError, self).__init__("Check the API response for error details.")
        self.response = api_response


def handle_response_with_items(response):
    if 200 <= response.status_code < 300:
        return response.items
    else:
        raise FlashArrayError(response)


def handle_response_with_value(response, value):
    if 200 <= response.status_code < 300:
        return value
    else:
        raise FlashArrayError(response)


def handle_response(response):
    if 200 <= response.status_code < 300:
        return
    else:
        raise FlashArrayError(response)
