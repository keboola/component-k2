import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import K2Client, K2ClientException

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        username = params.get(KEY_USERNAME)
        password = params.get(KEY_PASSWORD)
        source_url = params.get(KEY_PASSWORD)

        client = K2Client(username, password, source_url)

        try:
            client.get_endpoint("test")
        except K2ClientException as client_exc:
            raise UserException(client_exc) from client_exc


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
