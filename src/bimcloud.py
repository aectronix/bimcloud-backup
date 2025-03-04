import json
import logging
import requests
import sys
import time

from urllib3.util.retry import Retry

class BIMcloudAPI():

	def __init__(self, manager: str, client: str, user: str, password: str, **kwargs):
		"""
		Initialize the BIMcloudAPI instance.

		Args:
			manager (str): The BIMcloud Manager URL.
			client (str): The client identification.
			user (str): The username.
			password (str): The password.
		"""
		self.log = logging.getLogger("BackupManager")
		self.manager = manager
		self._client = client
		self._user = user
		self._password = password
		self._auth = None
		self._session = None

		self._r = self._setup_requests()
		self.authorize()

	def _setup_requests(self):
		"""
		Initialize a requests.Session with a retry adapter.

		Returns:
			requests.Session: Configured session.
		"""
		adapter = requests.adapters.HTTPAdapter(
			max_retries=Retry(
				total=3,
				backoff_factor=1,
				status_forcelist=[429, 500, 502, 503, 504],
				allowed_methods=['GET', 'POST', 'DELETE',]
			)
		)
		session = requests.Session()
		session.mount("https://", adapter)
		session.mount("http://", adapter)
		return session

	def _refresh_token(self):
		"""
		Refresh the auth token if it is about to expire.

		Raises:
			ValueError: If 'access_token_exp' is missing in auth data.
			requests.exceptions.RequestException: For any HTTP-related errors.
		"""
		now = time.time()
		access_token_exp = self._auth.get('access_token_exp')
		if access_token_exp is None:
			raise ValueError("Missing 'access_token_exp' in auth data.")
		if now >= access_token_exp - 10:
				response = self.oauth2_refresh()
				response.raise_for_status()
				auth = response.json()
				self._auth = auth

	def _send_request(self, method: str, url: str, **kwargs):
		"""
		Refresh the token/session if necessary and send an HTTP request.

		Args:
			method (str): HTTP method (e.g. 'GET', 'POST', 'DELETE').
			url (str): URL to request.
			**kwargs: Additional keyword arguments (e.g. timeout, stream, params, etc.).
			Use the 'stream' key to indicate if the raw response should be returned.

		Returns:
			The parsed JSON response or raw response based on the 'stream' flag.

		Raises:
			RuntimeError: If the HTTP response is not OK.
		"""
		self.refresh_on_expiration()
		headers_extra = kwargs.pop('headers', {})
		headers = {**{'Authorization': f"Bearer {self._auth.get('access_token')}"}, **headers_extra}
		response = self._r.request(method.upper(), url, headers=headers, **kwargs)
		return self._take_response(response, kwargs.get('stream', False))

	def _take_response(self, response: requests.Response, raw_stream: bool = False):
		"""
		Process the HTTP response.

		Args:
			response (requests.Response): The HTTP response.
			raw_stream (bool): If True, return the raw response; otherwise, parse JSON.

		Returns:
			Parsed JSON data if response has content and raw_stream is False;
			raw response if raw_stream is True;
			or None if there is no content.

		Raises:
			RuntimeError: If the HTTP response status is not OK.
		"""
		has_content = response.content is not None and len(response.content)
		if response.ok:
			if has_content:
				return response if raw_stream else response.json()
			else:
				return None
		raise RuntimeError(f"Response Error {response}")

	def authorize(self):
		"""
		Authorize in BIMcloud instance.

		Returns:
			tuple: (auth, session) dictionaries.

		Raises:
			RuntimeError: If authentication or session creation fails.
		"""
		try:
			response = self.oauth2(self._user, self._password, self._client)
			response.raise_for_status()
			auth = response.json()
			self.log.info(f"Connected to bimcloud on: {self.manager}")
		except Exception as e:
			self.log.error(f"Authentication error: {e}", exc_info=True)
			raise RuntimeError("Authentication failed") from e

		self._auth = auth

	def refresh_on_expiration(self):
		"""
		Refresh the auth token if expired.

		Raises:
			RuntimeError: If the refresh process fails.
		"""
		try:
			self._refresh_token()
			self.log.debug("Authentication token refreshed.")
		except requests.exceptions.RequestException as e:
			self.log.error(f"Refresh error: {e}", exc_info=True)
			raise RuntimeError("Refresh failed") from e

	def oauth2(self, user, password, client_id):
		""" Perform the OAuth2 authentication call. """
		request = {
			'grant_type': 'password',
			'username': user,
			'password': password,
			'client_id': client_id
		}
		url = self.manager + '/management/client/oauth2/token'
		response = self._r.post(url, data=request, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=30)
		return response

	def oauth2_refresh(self):
		""" Perform the OAuth2 token refresh call. """
		request = {
			'grant_type': 'refresh_token',
			'refresh_token': self._auth.get('refresh_token'),
			'client_id': self._client
		}
		url = self.manager + '/management/client/oauth2/token'
		response = self._r.post(url, data=request, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=30)
		return response

	def create_resource_backup(self, resource_id, backup_type, backup_name):
		""" Create a new backup for a resource. """
		url = self.manager + '/management/latest/create-resource-backup'
		response = self._send_request('post', url,  params={'resource-id': resource_id, 'backup-type': backup_type, 'backup-name': backup_name})
		return response

	def delete_resource_backup(self, resource_id, backup_id):
		""" Delete a specific resource backup. """
		url = self.manager + '/management/latest/delete-resource-backup'
		response = self._send_request('delete', url, params={'resource-id': resource_id, 'backup-id': backup_id})
		return response

	def delete_resource_backup_schedule(self, resource_id):
		""" Delete backup schedules for a resource. """
		url = self.manager + '/management/latest/delete-resource-backup-schedule'
		response = self._send_request('delete', url, params={'resource-id': resource_id})
		return response

	def get_jobs(self, criterion=None, params=None):
		""" Retrieve jobs based on given criteria. """
		url = self.manager + '/management/client/get-jobs-by-criterion'
		response = self._send_request('post', url, params=params, json=criterion)
		return response

	def download_backup(self, resource_id, backup_id, timeout=300, stream=False):
		""" Download a backup file from BIMcloud. """
		url = self.manager + '/management/client/download-backup'
		response = self._send_request('get', url, params={'resource-id': resource_id, 'backup-id': backup_id}, timeout=timeout, stream=stream)
		return response 

	def get_resources_by_criterion(self, criterion=None, params=None):
		""" Retrieve resources based on a given criterion. """
		url = self.manager + '/management/client/get-resources-by-criterion'
		response = self._send_request('post', url, params=params, json=criterion)
		return response

	def get_resources_by_id_list(self, ids, params=None):
		""" Retrieve resources by a list of IDs. """
		url = self.manager + '/management/client/get-resources-by-id-list'
		response = self._send_request('post', url, params=params, json=ids)
		return response

	def get_resource_backups(self, resources_ids, criterion=None, params=None):
		""" Retrieve backups for given resource IDs using specific criteria. """
		url = self.manager + '/management/client/get-resource-backups-by-criterion'
		response = self._send_request('post', url, params=params, json={'ids': resources_ids, 'criterion': criterion})
		return response

	def get_resource_backup_schedules(self, criterion=None):
		""" Retrieve backup schedules based on a given criterion. """
		url = self.manager + '/management/client/get-resource-backup-schedules-by-criterion'
		response = self._send_request('post', url, json=criterion)
		return response

	def insert_resource_backup_schedule(
			self,
			targetResourceId,
			backupType,
			enabled = True,
			maxBackupCount = 1,
			repeatInterval = 3600,
			repeatCount = 0,
			startTime = 0,
			endTime = 0,
			type = 'resourceBackupSchedule',
			revision = 0
		):
		""" Insert a backup schedule for a resource. """
		schedule = {
			'id': backupType + targetResourceId,
			'$hidden': False,
			'$visibility': 'full',
			'targetResourceId': targetResourceId,
			'backupType': backupType,
			'enabled': enabled,
			'maxBackupCount': maxBackupCount,
			'repeatInterval': repeatInterval,
			'repeatCount': repeatCount,
			'startTime': startTime,
			'endTime': endTime,
			'type': type,
			'revision': revision
		}
		url = self.manager + '/management/client/insert-resource-backup-schedule'
		response = self._send_request('post', url, json=schedule)
		return response