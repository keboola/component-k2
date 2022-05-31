import hmac
import base64
import urllib
import requests
import json
import math
from urllib.parse import unquote
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Generator, Dict, Optional
from keboola.http_client import HttpClient

BASE_URL = ""
PAGE_SIZE = 250


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
        response = requests.get(requests_url, headers=auth_header)
        if response.status_code != 200:
            raise K2ClientException(
                f"Failed to fetch object metadata because of error {response.status_code} : {response.text}")
        return json.loads(response.text)

    def estimate_amount_of_pages(self, object_name: str, pkey: str, fields: Optional[str],
                                 conditions: Optional[str]) -> str:
        parameters = self._generate_object_request_params(fields, conditions)
        requests_url = self._generate_object_request_url(object_name, parameters)
        auth_header = self._get_auth_header(self.username, self.password, requests_url)
        response = self.get_raw(requests_url, is_absolute_path=True, headers=auth_header)
        first_page_response_time = response.elapsed.total_seconds()
        self._handle_http_error(response)
        first_page = json.loads(response.text)
        if "LastPageURL" not in first_page:
            return "No Last page url available, only one page is available"
        auth_header = self._get_auth_header(self.username, self.password, first_page.get("LastPageURL"))
        last_page_response = self.get_raw(first_page.get("LastPageURL"), is_absolute_path=True, headers=auth_header)
        last_page_response_time = last_page_response.elapsed.total_seconds()
        last_page = json.loads(last_page_response.text)

        first_page_keys = []
        for item in first_page.get("Items"):
            first_page_keys.extend(field.get("Value") for field in item.get("FieldValues") if field.get("Name") == pkey)

        last_page_keys = []
        for item in last_page.get("Items"):
            last_page_keys.extend(field.get("Value") for field in item.get("FieldValues") if field.get("Name") == pkey)

        first_pkey_value = first_page_keys[0]
        last_pkey_value = last_page_keys[-1]

        if not isinstance(first_pkey_value, int):
            return f"First page primary key value is {first_pkey_value}, last page pkey value is {last_pkey_value}"

        gradient_first_page = (first_page_keys[-1] - first_page_keys[0]) / (len(first_page_keys))
        gradient_last_page = (last_page_keys[-1] - last_page_keys[0]) / (len(last_page_keys))
        average_gradient = (gradient_last_page + gradient_first_page) / 2
        expected_number_of_entries = (last_pkey_value - first_pkey_value) / average_gradient
        expected_number_of_pages = math.ceil(expected_number_of_entries / PAGE_SIZE)
        expected_time_for_fetching = math.ceil(expected_number_of_pages * (
                (first_page_response_time + last_page_response_time) / 2))
        return f"The expected amount of pages is {expected_number_of_pages}, " \
               f"this should take about {expected_time_for_fetching} seconds"

    def get_object_data(self, object_name: str, fields: Optional[str], conditions: Optional[str]) -> Generator:
        parameters = self._generate_object_request_params(fields, conditions)
        requests_url = self._generate_object_request_url(object_name, parameters)
        auth_header = self._get_auth_header(self.username, self.password, requests_url)

        last_page = False
        next_page_url = ""
        while not last_page:
            if next_page_url:
                auth_header = self._get_auth_header(self.username, self.password, next_page_url)
                response = self.get_raw(next_page_url, is_absolute_path=True, headers=auth_header)
            else:
                response = self.get_raw(requests_url, is_absolute_path=True, headers=auth_header)
            self._handle_http_error(response)
            current_page = json.loads(response.text)
            next_page_url = current_page.get("NextPageURL")
            if not next_page_url:
                last_page = True
            yield current_page.get("Items")

    def _get_auth_header(self, username: str, password: str, source_url: str) -> Dict:
        hmac_hash = self._generate_hmac_hash(password, source_url)
        return {"Authorization": f"{username}:{hmac_hash}"}

    @staticmethod
    def _generate_object_request_params(fields: Optional[str], conditions: Optional[str]) -> Dict:
        parameters = {"pageSize": PAGE_SIZE}
        if fields:
            parameters["fields"] = fields
        if conditions:
            parameters["conditions"] = conditions
        return parameters

    def _generate_object_request_url(self, object_name: str, parameters: Dict) -> str:
        encoded_params = urllib.parse.urlencode(parameters)
        return f"{self.base_url}Data/{object_name}?{encoded_params}"

    @staticmethod
    def _generate_hmac_hash(password: str, source_url: str) -> str:
        unquoted_source_url = unquote(source_url)
        message = unquoted_source_url.upper().encode()
        key = password.encode()
        hmac_hash = hmac.new(key, msg=message, digestmod='md5')
        base_44_encoded_hash = base64.b64encode(hmac_hash.digest())
        return base_44_encoded_hash.decode()

    @staticmethod
    def _handle_http_error(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            response_error = json.loads(e.response.text)
            if response_error.get('code') == 400:
                raise K2ClientException("Failed to process object query, " f"either invalid object or fields. "
                                        f"{response_error.get('error')}") from e

            if response_error.get('code') == 401:
                raise K2ClientException("Failed to Authorize the component, make sure your "
                                        f"credentials and source ur are valid. {response_error.get('error')}") from e

            raise K2ClientException(
                f"{response_error.get('error')}. Exception code {response_error.get('code')}") from e

    # override to continue on failure
    def _requests_retry_session(self, session=None) -> requests.Session:
        session = session or requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
