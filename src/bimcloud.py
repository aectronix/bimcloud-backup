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
		self._auth, self._session = self.authorize()

	def _setup_requests(self):
		"""
		Initialize a requests.Session with a retry adapter.

		Returns:
			requests.Session: Configured session.
		"""
		adapter = requests.adapters.HTTPAdapter(
			max_retries=Retry(
				total=1,
				backoff_factor=1,
				status_forcelist=[429, 430, 500, 502, 503, 504],
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
				response = self.oauth2_refresh(self.auth.get('refresh_token'))
				response.raise_for_status()
				auth = response.json()
				self._auth = auth

	def _refresh_session(self):
		"""
		Refresh the session if it has expired.

		Raises:
			ValueError: If 'expire-timeout' or 'timestamp' is missing or session response is invalid.
			requests.exceptions.RequestException: For any HTTP-related errors.
		"""
		now = time.time()
		timestamp = self._session.get('timestamp')
		expire_timeout = self._session.get('expire-timeout')
		if timestamp is None or expire_timeout is None:
			raise ValueError("Missing 'timestamp' or 'expire-timeout' in session data.")
		if now - timestamp >= expire_timeout:
			response = self.create_session()
			response.raise_for_status()
			session_data = response.json()
			if session_data and session_data.get('session-id'):
				session_data['timestamp'] = now
				self._session = session_data
			else:
				raise ValueError("Invalid session response during refresh.")

	def _send_request(self, method: str, url: str, *args, **kwargs):
		"""
		Refresh the token/session if necessary and send an HTTP request.

		Args:
			method (str): HTTP method (e.g. 'GET', 'POST', 'DELETE').
			url (str): URL to request.
			*args: Additional positional arguments.
			**kwargs: Additional keyword arguments.

		Returns:
			The parsed JSON response or raw content based on _take_response.

		Raises:
			HttpError: If the HTTP response is not OK.
		"""
		self.refresh_on_expiration()
		headers = kwargs.pop('headers', {})
		headers = {'Authorization': f"Bearer {self._auth.get('access_token')}", **headers}
		response = self._r.request(method, url, headers=headers, **kwargs)
		return self._take_response(response)

	def _take_response(self, response: requests.Response, json=True):
		"""
		Process the HTTP response.

		Args:
			response (requests.Response): The HTTP response.
			json (bool): Whether to parse JSON. If False, return raw content.

		Returns:
			Parsed JSON or raw content if response has content; otherwise, None.

		Raises:
			HttpError: If the response is not OK.
		"""
		has_content = response.content is not None and len(response.content)
		if response.ok:
			if has_content:
				return response.json() if json else response.content
			else:
				return None
		raise HttpError(response)

	def authorize(self):
		"""
		Authorize with BIMcloud and create a session.

		Returns:
			tuple: (auth, session) dictionaries.

		Raises:
			RuntimeError: If authentication or session creation fails.
		"""
		try:
			response = self.oauth2(self._user, self._password, self._client)
			response.raise_for_status()
			auth = response.json()
		except Exception as e:
			self.log.error(f"Authentication error: {e}", exc_info=True)
			raise RuntimeError("Authentication failed") from e

		try:
			response = self.create_session()
			response.raise_for_status()
			session = response.json()
			session['timestamp'] = time.time()

		except Exception as e:
			self.log.error(f"Session error: {e}", exc_info=True)
			raise RuntimeError("Session creation failed") from e

		return auth, session

	def refresh_on_expiration(self):
		"""
		Refresh the auth token and session if they are expired.

		Raises:
			RuntimeError: If the refresh process fails.
		"""
		try:
			self._refresh_token()
			self._refresh_session()
			self.log.debug("Auth token & session have been refreshed.")
		except requests.exceptions.RequestException as e:
			self.log.error(f"Refresh error: {e}", exc_info=True)
			raise RuntimeError("Refresh failed") from e


	def test(self):
		url = self.manager + '/get-server-info'
		test = self._send_request('get', url)
		print (test)


	def oauth2(self, user, password, client_id):
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
		request = {
			'grant_type': 'refresh_token',
			'refresh_token': self._auth.get('refresh_token'),
			'client_id': self.client
		}
		url = self.manager + '/management/client/oauth2/token'
		response = self._request_session.post(url, data=request, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=30)
		return response

	def create_session(self):
		request = {
			'username': self._user,
			'password': self._password,
			'client-id': self._client
		}
		url = self.manager + '/management/latest/create-session'
		response = requests.post(url, json=request)
		return response

	def close_session(self, session_id=None):
		if not session_id:
			session_id = self._session.get('session-id')
		url = self.manager + '/management/latest/close-session'
		response = requests.post(url, params={'session-id': session_id})
		return response

	def ping_session(self, session_id=None):
		if not session_id:
			session_id = self._session.get('session-id')
		url = self.manager + '/management/latest/ping-session'
		response = requests.post(url, params={'session-id': session_id})
		return response

	def create_resource_backup(self, resource_id, backup_type, backup_name):
		url = self.manager + '/management/latest/create-resource-backup'
		response = requests.post(url, headers=self.auth_header, params={'resource-id': resource_id, 'backup-type': backup_type, 'backup-name': backup_name})
		return response.json()

	def delete_resource_backup(self, resource_id, backup_id):
		url = self.manager + '/management/latest/delete-resource-backup'
		response = requests.delete(url, headers=self.auth_header, params={'resource-id': resource_id, 'backup-id': backup_id})
		return response

	def delete_resource_backup_schedule(self, resource_id):
		url = self.manager + '/management/latest/delete-resource-backup-schedule'
		response = requests.delete(url, headers=self.auth_header, params={'resource-id': resource_id})
		return response

	def get_jobs(self, criterion={}, params={}):
		params = params or {}
		url = self.manager + '/management/client/get-jobs-by-criterion'
		response = requests.post(url, headers=self.auth_header, params=params, json=criterion)
		return response

	def download_backup(self, resource_id, backup_id, timeout=300, stream=False):
		url = self.manager + '/management/client/download-backup'
		response = requests.get(url, params={'session-id': self.session['session-id'], 'resource-id': resource_id, 'backup-id': backup_id}, timeout=timeout, stream=stream)
		return response 

	def get_resources_by_criterion(self, criterion={}, params={}):
		url = self.manager + '/management/client/get-resources-by-criterion'
		response = requests.post(url, headers=self.auth_header, params=params, json=criterion)
		return response.json()

	def get_resources_by_id_list(self, ids, params):
		url = self.manager + '/management/client/get-resources-by-id-list'
		response = self._send_request('post', url, json=ids)
		return response

	def get_resource_backups(self, resources_ids, criterion=None, params=None):
		url = self.manager + '/management/client/get-resource-backups-by-criterion'
		response = self._send_request('post', url, params=params, json={'ids': resources_ids, 'criterion': criterion})
		return response

	def get_resource_backup_schedules(self, criterion=None):
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
		schedule = {
			'id': backupType+targetResourceId,
			'$hidden': False,
			"$visibility": 'full'
		}
		schedule = {key: value for key, value in locals().items() if key not in ('self')}
		url = self.manager + '/management/client/insert-resource-backup-schedule'
		response = requests.post(url, headers=self.auth_header, json=schedule)
		return response