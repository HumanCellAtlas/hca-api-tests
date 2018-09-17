import copy
import json
import logging
import os
import sys
import time
from collections import deque

import requests
from locust import TaskSet, HttpLocust, task

# sys.path setup needs to happen before import of common module
sys.path.append(os.getcwd())
from common.auth0 import Authenticator

DEFAULT_FILE_UPLOAD_URL = 'http://localhost:8888/v1'

BASE_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
FILE_DIRECTORY = f'{BASE_DIRECTORY}/files/secondary_analysis'


class Resource(object):

    _links = None
    _content = None

    def __init__(self, links):
        self._links = links

    @staticmethod
    def from_json(source):
        resource = Resource(source.get('_links'))
        resource._content = source.get('content')
        return resource

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


_authenticator = Authenticator()


class CoreClient:

    def __init__(self, client):
        self.client = client

    def create_submission(self) -> Resource:
        headers = {'Authorization': f'Bearer {_authenticator.get_token()}'}
        response = self.client.post('/submissionEnvelopes', headers=headers, json={},
                                    name='create new submission')
        response_json = response.json()
        links = response_json.get('_links')
        submission = None
        if links:
            submission = Resource(links)
        return submission


class SecondarySubmission(TaskSet):

    _core_client = None

    def on_start(self):
        self._core_client = CoreClient(self.client)
        self._setup_input_bundle()

    def _setup_input_bundle(self):
        core_client = CoreClient(self.client)
        submission = core_client.create_submission()
        files_link = submission.get_link('files')
        for count in range(0, 2):
            file_name = f'input_file_{"%02d" % count}.fastq.gz'
            dummy_file = _create_test_file(file_name)
            response = self.client.post(files_link, json=dummy_file, name='set up input file')
            logging.info('Accessioning input file...')
            file_resource = Resource(response.json().get('_links'))
            file = self._retrieve_file(file_resource.get_link('self'))

    def _retrieve_file(self, file_link):
        file = None
        file_json = None
        group_name = 'retrieve file; wait for accessioning'
        while not (file_json and file_json['uuid']):
            file_json = self.client.get(file_link, name=group_name).json()
            if file_json:
                file = Resource.from_json(file_json)
            else:
                time.sleep(.500)
        return file

    def on_stop(self):
        _authenticator.end_session()
        _submission_queue.clear()
        _analysis_queue.clear()

    @task
    def setup_analysis(self):
        submission = self._core_client.create_submission()
        if submission:
            _submission_queue.queue(submission)
            self._add_analysis_to_submission(submission)

    def _add_analysis_to_submission(self, submission: Resource):
        processes_link = submission.get_link('processes')
        response = self.client.post(processes_link, json=_dummy_analysis,
                                    name='add analysis to submission')
        analysis_json = response.json()
        links = analysis_json.get('_links')
        if links:
            analysis = Resource(links)
            _analysis_queue.queue(analysis)
            self._add_file_reference(analysis)

    def _add_file_reference(self, analysis: Resource):
        file_reference_link = analysis.get_link('add-file-reference')
        for dummy_analysis_file in _dummy_analysis_files:
            self.client.put(file_reference_link, json=dummy_analysis_file,
                            name="add file reference")


class FileUpload(TaskSet):

    def on_start(self):
        pass

    @task
    def upload_files(self):
        submission = _submission_queue.wait_for_resource()
        upload_area_uuid = None
        submission_link = submission.get_link('self')
        while not upload_area_uuid:
            upload_area_uuid = self._get_upload_area_uuid(submission_link)
            if not upload_area_uuid:
                time.sleep(3)
        FileUpload._upload_dummy_files(upload_area_uuid)

    def _get_upload_area_uuid(self, submission_link):
        upload_area_uuid = None
        submission_data = self.client.get(submission_link, name='get submission data').json()
        staging_details = submission_data.get('stagingDetails')
        if staging_details:
            staging_area_uuid = staging_details.get('stagingAreaUuid')
            if staging_area_uuid:
                upload_area_uuid = staging_area_uuid.get('uuid')
        return upload_area_uuid

    @staticmethod
    def _upload_dummy_files(upload_area_uuid):
        upload_url = f'{_file_upload_base_url}/area/{upload_area_uuid}/files'
        logging.info(f'uploading dummy files to [{upload_url}]...')
        for _dummy_analysis_file in _dummy_analysis_files:
            file_json = {
                'fileName': _dummy_analysis_file['fileName'],
                'contentType': 'application/tar+gzip;dcp-type=data'
            }
            requests.put(upload_url, json=file_json)


class GreenBox(HttpLocust):
    task_set = SecondarySubmission


class FileUploader(HttpLocust):
    task_set = FileUpload
