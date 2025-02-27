import json
import logging
import requests
import sys

from urllib3.util.retry import Retry

class BIMcloudAPI():

	def __init__(self, manager: str, client: str, user: str, password: str, **params):
		self.log = logging.getLogger("BackupManager")
		self.manager = manager
		self.client = client
		self.user = user
		self.password = password
		self.auth = None
		self.auth_header = None
		self.session = None

		self.authorize()

	def authorize(self, refresh=False):
		url = self.manager + '/management/client/oauth2/token'
		request = {
			'grant_type': 'password',
			'username': self.user,
			'password': self.password,
			'client_id': self.client
		}

		retry_strategy = Retry(
			total=1,
			backoff_factor=1,
			status_forcelist=[429, 430, 500, 502, 503, 504],
			allowed_methods=['GET', 'POST']
		)
		adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
		session = requests.Session()
		session.mount("https://", adapter)
		session.mount("http://", adapter)

		try:
			response = session.post(url, data=request, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=30)
			response.raise_for_status()
			result = response.json()
			if result and result.get('access_token'):
				self.auth = result
				self.auth_header = {'Authorization': f"Bearer {result['access_token']}"}
			auth_session = self.create_session(self.user, self.password, self.client)
			if auth_session and auth_session.get('session-id'):
				self.session = auth_session
			if not refresh:
				self.log.info(f"BIM cloud initialized: {self.manager} ({self.user})")
		except Exception as e:
			self.log.error(f"Auth error: {e}", exc_info=True)
			sys.exit(1)

	def refresh(self):
		self.authorize(refresh=True)

	def create_session(self, username, password, client_id):
		request = {
			'username': username,
			'password': password,
			'client-id': client_id
		}
		url = self.manager + '/management/latest/create-session'
		response = requests.post(url, json=request)
		return response.json()

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
		response = requests.post(url, headers=self.auth_header, params=params, json=ids)
		return response.json()

	def get_resource_backups(self, resources_ids, criterion={}, params={}):
		url = self.manager + '/management/client/get-resource-backups-by-criterion'
		response = requests.post(url, headers=self.auth_header, params=params, json={'ids': resources_ids, 'criterion': criterion})
		return response.json()

	def get_resource_backup_schedules(self, criterion={}):
		url = self.manager + '/management/client/get-resource-backup-schedules-by-criterion'
		response = requests.post(url, headers=self.auth_header, params={}, json=criterion)
		return response.json()

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