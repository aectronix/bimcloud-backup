import argparse
import json
import requests
import time

from datetime import date, datetime, timedelta

class BIMcloud():

	def __init__(self, manager: str, client: str, user: str, password: str, **params):
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
			result = response.json() if json else response.content
			self.auth = result
			self.auth_header = {'Authorization': f"Bearer {result['access_token']}"}
		except:
			raise

	def _grant(self):
		try:
			servers = self.get_model_servers()
			ticket = self.get_ticket(servers[0]['id'])
			session = self.create_session(self.user, ticket)
			if session:
				self.session = session
		except:
			raise

	def begin_batch_upload(self, session_id, description='batch-upload'):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/begin-batch-upload'
		response = requests.post(url, params={'session-id': session_id, 'description': description})
		return response.json()['data']

	def begin_upload(self, session_id, blob_name, namespace):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/begin-upload'
		response = requests.post(url, params={'session-id': session_id, 'blob-name': blob_name, 'namespace-name': namespace})
		return response.json()['data']

	def commit_batch_upload(self, session_id, batch_id):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/commit-batch-upload'
		response = requests.post(url, params={'session-id': session_id, 'batch-upload-session-id': batch_id, 'conflict-behavior': 'overwrite'})
		return response

	def commit_upload(self, session_id, upload_id):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/commit-upload'
		response = requests.post(url, params={'session-id': session_id, 'upload-session-id': upload_id})
		return response

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

	def get_blob_content(self, session_id, blob_id):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/get-blob-content'
		response = requests.get(url, params={'session-id': session_id, 'blob-id': blob_id}, stream=True)
		return response

	def get_jobs(self, criterion={}, params={}):
		url = self.manager + '/management/client/get-jobs-by-criterion'
		response = requests.post(url, headers=self.auth_header, params=params, json=criterion)
		return response.json()

	def get_model_servers(self):
		url = self.manager + '/management/client/get-model-servers'
		response = requests.get(url, headers=self.auth_header)
		return response.json()

	def get_resources(self, criterion={}):
		url = self.manager + '/management/client/get-resources-by-criterion'
		response = requests.post(url, headers=self.auth_header, params={}, json={**criterion})
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

	def put_blob_content_part(self, session_id, upload_id, data, offset=0):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/put-blob-content-part'
		request = {
			'session-id': session_id,
			'upload-session-id': upload_id,
			'offset': offset,
			'length': len(data)
		}
		response = requests.post(url, params=request, data=data)

class BackupManager():

	def __init__(self, client):
		self.client = client




	def backup(self):
		"""	Starts resource backup procedure.
		"""
		# resources = self.client.get_resources({'$eq': {'type': 'project'}}, )
		resources = self.client.get_resources({'$eq': {'id': '1A787BDC-5498-4A87-A254-FD1F9991F20C'}}, )
		for r in resources:
			has_valid_backup = False
			backups = self.client.get_resource_backups([r['id']], params={'sort-by': '$time', 'sort-direction': 'desc'})
			if backups[0] and backups[0].get('$time') >= r['$modifiedDate']:
				has_valid_backup = True				

			if not has_valid_backup and r['type'] == 'project':
				# create new backup
				create = self.create_project_backup(r['id'])
				# remove all obsolete backups
				for b in backups:
					if b['$time'] <= r['$modifiedDate']:
						delete = self.client.delete_resource_backup(r['id'], b['id'])

	def create_project_backup(self, resource_id):
		"""	Creates a new backup for project resource if necessary.
			Args:
				resource_id (str): Resource id
			Returns:
				bool: True if process succeded
		"""
		print (resource_id)
		response, job = None, None
		response = self.client.create_resource_backup(resource_id, 'bimproject', 'Scripted Backup 1')
		if response and response.get('id'):
			job = response
			while job['status'] not in ['completed', 'failed']:
				job = self.client.get_jobs(
					criterion = {
						'$and': [
							{'$eq': {'jobType': 'createProjectBackup'}},
							{'$eq': {'id': response['id']}}
						]
					}
				)[0]
				print (f"{job['status']}: {job['progress']['current']} / {job['progress']['max']} ",  end='\r')
				time.sleep(1.5)

		if job['status'] == 'completed':
			return True
		return False

	def delete_schedules(self):
		d = 0
		resources = self.client.get_resources({'$eq': {'type': 'project'}})
		for r in resources:
			schedules = self.client.get_resource_backup_schedules({'$eq': { 'targetResourceId': r['id']}})
			if schedules:
				for s in schedules:
					delete = self.client.delete_resource_backup_schedule(s['id'])
					# print (f"Deleted: {s['id']} {delete}")
					d =+ 1
		print (f"Deleted: {d} backup schedules")



	def write_data(self, data, path):
		# convert data
		binary = data.encode('utf-8')

		# begin batch & upload
		batch = self.client.begin_batch_upload(self.client.session['id'], description=f"Uploading to {path}")
		upload = self.client.begin_upload(self.client.session['id'], path, batch['namespace-name'])
		
		# write
		CHUNK_SIZE = 1024 * 512
		offset = 0
		while offset < len(binary):
			chunk = binary[offset:offset + CHUNK_SIZE]
			self.client.put_blob_content_part(self.client.session['id'], upload['id'], chunk, offset=offset)
			offset += CHUNK_SIZE

		# commit
		commit_upload = self.client.commit_upload(self.client.session['id'], upload['id'])
		commit_batch = self.client.commit_batch_upload(self.client.session['id'], batch['id'])
		print(f"Upload: {path} {commit_batch}")


if __name__ == "__main__":

	cmd = argparse.ArgumentParser()
	cmd.add_argument('-m', '--manager', required=True, help='URL of the BIMcloud Manager')
	cmd.add_argument('-c', '--client', required=True, help='Client Identification')
	cmd.add_argument('-u', '--user', required=True, help='User Login')
	cmd.add_argument('-p', '--password', required=True, help='User Password')
	cmd.add_argument('-b', '--disable_schedules', required=False, help='Disable backup schedules')
	arg = cmd.parse_args()

	try:
		bc = BIMcloud(**vars(arg))
		bm = BackupManager(bc)

		# ensure remove any new backup schedule, if enabled
		if arg.disable_schedules == 'y':
			ds = bm.delete_schedules()

		bcp = bm.backup()



	except:
		raise