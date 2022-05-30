import hmac
import base64
from urllib.parse import unquote
from keboola.http_client import HttpClient
import urllib
from typing import Optional

from typing import Generator, Dict

BASE_URL = ""


class K2ClientException(Exception):
    pass


class K2Client(HttpClient):
    def __init__(self, username: str, password: str, k2_address: str, service_name: str) -> None:
        self.username = username
        self.password = password
        self.k2_address = k2_address
        self.service_name = service_name
        super().__init__(f"{k2_address}/{service_name}", max_retries=3)

    def get_object_meta(self, object_name: str) -> Dict:
        requests_url = f"{self.base_url}Meta/{object_name}"
        auth_header = self._get_auth_header(self.username, self.password, requests_url)
        header = {"Authorization": auth_header}
        return self.get(requests_url, is_absolute_path=True, headers=header)  # noqa returns Dict even though typehint is requests.Response

    def get_object_data(self, object_name: str, fields: Optional[str], conditions: Optional[str]) -> Generator:
        parameters = {"pageSize": 250}
        if fields:
            parameters["fields"] = fields
        if conditions:
            parameters["conditions"] = conditions

        encoded_params = urllib.parse.urlencode(parameters)
        requests_url = f"{self.base_url}Data/{object_name}?{encoded_params}"

        auth_header = self._get_auth_header(self.username, self.password, requests_url)
        header = {"Authorization": auth_header}
        last_page = False
        next_page_url = ""
        while not last_page:
            if next_page_url:
                auth_header = self._get_auth_header(self.username, self.password, next_page_url)
                header = {"Authorization": auth_header}
                current_page = self.get(next_page_url, is_absolute_path=True, headers=header)
            else:
                current_page = self.get(requests_url, is_absolute_path=True, headers=header)
            next_page_url = current_page.get("NextPageURL")
            if not next_page_url:
                last_page = True
            yield current_page.get("Items")

    def _get_auth_header(self, username: str, password: str, source_url: str) -> str:
        hmac_hash = self._generate_hmac_hash(password, source_url)
        return f"{username}:{hmac_hash}"

    @staticmethod
    def _generate_hmac_hash(password: str, source_url: str) -> str:
        unquoted_source_url = unquote(source_url)
        message = unquoted_source_url.upper().encode()
        key = password.encode()
        hmac_hash = hmac.new(key, msg=message, digestmod='md5')
        base_44_encoded_hash = base64.b64encode(hmac_hash.digest())
        return base_44_encoded_hash.decode()
