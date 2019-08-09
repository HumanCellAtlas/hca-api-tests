from ingest.utils.token_manager import TokenManager
from ingest.utils.s2s_token_client import S2STokenClient

import logging

DEFAULT_KEY_FILE_PATH = "/data/secret/key.json"


class Authenticator:

    def __init__(self, private_key_file_path):
        self._access_token = None
        self.token_manager = Authenticator.setup_token_manager(private_key_file_path)

    def start_session(self):
        logging.info('Starting auth session...')
        self._access_token = self.token_manager.get_token()

    def get_token(self):
        return self.token_manager.get_token()

    def end_session(self):
        logging.info('Stopping auth session...')

    @staticmethod
    def setup_token_manager(private_key_file_path) -> TokenManager:
        token_client = S2STokenClient()
        token_client.setup_from_file(private_key_file_path)
        token_manager = TokenManager(token_client)
        return token_manager

    @staticmethod
    def default_authenticator():
        """
        Initiate an authenticate from the default key file location
        :return: an unconfigured Authenticator instance
        """
        return Authenticator(DEFAULT_KEY_FILE_PATH)
