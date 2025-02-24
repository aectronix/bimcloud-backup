import json
import logging
import requests
import sys

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

		self._authorize()
		self._grant()

	def _authorize(self):
		url = self.manager + '/management/client/oauth2/token'
		request = {
			'grant_type': 'password',
			'username': self.user,
			'password': self.password,
			'client_id': self.client
		}
		try:
			response = requests.post(url, data=request, headers={'Content-Type': 'application/x-www-form-urlencoded'})
			response.raise_for_status()
			result = response.json()
			self.auth = result
			self.auth_header = {'Authorization': f"Bearer {result['access_token']}"}
		except Exception as e:
			self.log.error(f"Auth error: {e}", exc_info=True)
			sys.exit(1)

	def _grant(self):
		try:
			servers = self.get_model_servers()
			ticket = self.get_ticket(servers[0]['id'])
			session = self.create_session2(self.user, self.password, self.client)
			print (session)
			if session:
				self.session = session
		except Exception as e:
			self.log.error(f"Unexpected error: {e}", exc_info=True)
			sys.exit(1)

	def create_session(self, username, ticket):
		request = {
			'data-content-type': 'application/vnd.graphisoft.teamwork.session-service-1.0.authentication-request-1.0+json',
			'data': {
				'username': username,
				'ticket': ticket
			}
		}
		# switch to XX001 server port
		url = self.manager[:-1] + '1' + '/session-service/1.0/create-session'
		response = requests.post(url, json=request, headers={'content-type': request['data-content-type']})
		result = response.json()
		return result['data']

	def create_session2(self, username, password, client_id):
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
		return response.json()

	def download_backup(self, resource_id, backup_id):
		url = self.manager + '/management/client/download-backup'
		response = requests.get(url, params={'session-id': self.session['session-id'], 'resource-id': resource_id, 'backup-id': backup_id})
		return response.content

	def get_model_servers(self):
		url = self.manager + '/management/client/get-model-servers'
		response = requests.get(url, headers=self.auth_header)
		return response.json()

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

	def get_ticket(self, server_id):
		url = self.manager + '/management/latest/ticket-generator/get-ticket'
		payload = {
			'type': 'freeTicket',
			'resources': [server_id],
			'format': 'base64'
		}
		response = requests.post(url, headers=self.auth_header, json=payload)
		return response.content.decode('utf-8')

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