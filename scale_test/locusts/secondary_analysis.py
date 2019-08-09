import copy
import json
import os
import sys
import time
from collections import deque

import logging
import requests
from locust import TaskSet, HttpLocust, TaskSequence, task, seq_task

# sys.path setup needs to happen before import of common module
sys.path.append(os.getcwd())
from scale_test.common.auth0 import Authenticator

DEFAULT_FILE_UPLOAD_URL = 'http://localhost:8070/v1'
DEFAULT_KEY_FILE_PATH = '/data/secrets/key.json'

BASE_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
FILE_DIRECTORY = f'{BASE_DIRECTORY}/files/secondary_analysis'


class Resource(object):

    _links = None
    _data = None

    def __init__(self, data, links):
        self._data = data
        self._links = links

    def get_link(self, path):
        return self._links[path]['href']


class ResourceQueue:

    def __init__(self):
        self._queue = deque()

    def queue(self, resource: Resource):
        self._queue.append(resource)

    def wait_for_resource(self):
        submission = self._queue.popleft() if len(self._queue) > 0 else None
        while not submission:
            time.sleep(0.5)
            submission = self._queue.popleft() if len(self._queue) > 0 else None
        return submission

    def clear(self):

        if len(self._queue) > 0:
            self._queue.clear()


_submission_queue = ResourceQueue()
_analysis_queue = ResourceQueue()

with open(f'{FILE_DIRECTORY}/analysis.json') as analysis_file:
    _dummy_analysis = json.load(analysis_file)
    
with open(f'{FILE_DIRECTORY}/analysis.json') as analysis_file:
    _dummy_analysis = json.load(analysis_file)

_file_template = {
    'fileName': '',
    'content': {
        'describedBy': 'https://schema.humancellatlas.org/type/file/6.1.1/sequence_file',
        'schema_type': 'file',
        'read_index': 'read1',
        'lane_index': 1
    }
}


def _create_test_file(name):
    test_file = copy.copy(_file_template)
    test_file['fileName'] = name
    split = name.split('.', 1)
    format = split[1] if len(split) > 1 else None
    test_file['content']['file_core'] = {'file_name': name, 'file_format': format}
    return test_file


_dummy_analysis_files = []
_base_name = 'ERR16300'
for index in range(1, 31):
    name = f'{_base_name}{"%02d" % index}.fastq.gz'
    _dummy_analysis_files.append(_create_test_file(name))


_file_upload_base_url = os.environ.get('FILE_UPLOAD_URL', DEFAULT_FILE_UPLOAD_URL)
_private_key_file_path = os.environ.get('KEY_FILE_PATH', DEFAULT_KEY_FILE_PATH)

_authenticator = Authenticator(_private_key_file_path)


class CoreClient:

    def __init__(self, client):
        self.client = client

    def create_submission(self, name='create new submission') -> Resource:
        headers = {'Authorization': f'Bearer {_authenticator.get_token()}'}
        response = self.client.post('/submissionEnvelopes', headers=headers, json={}, name=name)
        return CoreClient.parse_response(response)

    def create_metadata(self, create_link, name='create metadata') -> Resource:
        headers = {'Authorization': f'Bearer {_authenticator.get_token()}'}
        response = self.client.post(create_link, headers=headers, json={}, name=name)
        return CoreClient.parse_response(response)

    def add_output_file_to_process(self, create_link, file_json, name='add output file') -> Resource:
        headers = {'Authorization': f'Bearer {_authenticator.get_token()}'}
        response = self.client.put(create_link, headers=headers, json=file_json, name=name)
        return CoreClient.parse_response(response)

    @staticmethod
    def parse_response(response) -> Resource:
        response_json = response.json()
        links = response_json.get('_links')
        resource = None
        if links:
            resource = Resource(response_json, links)
        return resource


class SubmitAnalysisMetadata(TaskSequence):

    _core_client: CoreClient
    _submission: Resource
    _analysis_process: Resource

    def on_start(self):
        self._core_client = CoreClient(self.client)
        self._submission = None
        self._analysis_process = None

    def on_stop(self):
        _authenticator.end_session()
        _submission_queue.clear()
        _analysis_queue.clear()

    @task
    @seq_task(1)
    def create_analysis_submission(self):
        submission = self._core_client.create_submission(name='create analysis submission')
        self._submission = submission

    @task
    @seq_task(2)
    def add_analysis_process_to_submission(self):
        processes_link = self._submission.get_link('processes')
        self._analysis_process = self._core_client.create_metadata(processes_link, name='create analysis process')

    @task  # 30 files per analysis process
    @seq_task(3)
    def add_file_reference_to_analysis_process(self):
        file_reference_link = self._analysis_process.get_link('add-file-reference')
        for dummy_analysis_file in _dummy_analysis_files:
            self._core_client.add_output_file_to_process(file_reference_link,
                                                         dummy_analysis_file,
                                                         name="add analysis output file")

    @task
    @seq_task(4)
    def upload_analysis_files(self):
        submission = self._submission
        upload_area_uuid = None
        submission_link = submission.get_link('self')
        while not upload_area_uuid:
            upload_area_uuid = self._get_upload_area_uuid(submission_link)
            if not upload_area_uuid:
                time.sleep(3)
        self._upload_dummy_files(upload_area_uuid)

    def _get_upload_area_uuid(self, submission_link):
        upload_area_uuid = None
        submission_data = self.client.get(submission_link, name='get submission data').json()
        staging_details = submission_data.get('stagingDetails')
        if staging_details:
            staging_area_uuid = staging_details.get('stagingAreaUuid')
            if staging_area_uuid:
                upload_area_uuid = staging_area_uuid.get('uuid')
        return upload_area_uuid

    def _upload_dummy_files(self, upload_area_uuid):
        upload_url = f'{_file_upload_base_url}/area/{upload_area_uuid}/files'
        logging.info(f'uploading dummy files to [{upload_url}]...')
        for _dummy_analysis_file in _dummy_analysis_files:
            file_json = {
                'fileName': _dummy_analysis_file['fileName'],
                'contentType': 'application/tar+gzip;dcp-type=data'
            }
            requests.put(upload_url, json=file_json)


class SecondarySubmission(TaskSequence):
    tasks = [SubmitAnalysisMetadata]


class GreenBoxUser(HttpLocust):
    task_set = SecondarySubmission
