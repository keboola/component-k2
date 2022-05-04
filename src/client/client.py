import hmac
import base64
from urllib.parse import unquote
from keboola.http_client import HttpClient
from requests.exceptions import HTTPError

BASE_URL = ""


class K2ClientException(Exception):
    pass


class K2Client(HttpClient):
    def __init__(self, username: str, password: str, source_url: str):
        self.auth = self._get_auth_header(username, password, source_url)
        super().__init__(BASE_URL)

    def _get_auth_header(self, username: str, password: str, source_url: str) -> str:
        hmac_hash = self._generate_hmac_hash(password, source_url)
        return f"{username}:{hmac_hash}"

    @staticmethod
    def _generate_hmac_hash(password: str, source_url: str):
        source_url_decoded = unquote(source_url)
        hmac_hash = hmac.new(password.encode('utf-8'), source_url_decoded.encode('utf-8'), 'MD5')
        return base64.b64encode(hmac_hash.digest()).decode()

    def get_endpoint(self, endpoint):
        try:
            return self.get(endpoint_path=endpoint)
        except HTTPError as http_err:
            raise K2ClientException(http_err) from http_err
