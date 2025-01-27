import argparse
import json
import requests

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

	def delete_resource_backup(self, resource_id, backup_id):
		url = self.manager + '/management/latest/delete-resource-backup'
		response = requests.delete(url, headers=self.auth_header, params={'resource-id': resource_id, 'backup-id': backup_id})
		return response

	def get_blob_content(self, session_id, blob_id):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/get-blob-content'
		response = requests.get(url, params={'session-id': session_id, 'blob-id': blob_id}, stream=True)
		return response

	def get_resource_backups(self, resources_ids, criterion={}):
		url = self.manager + '/management/client/get-resource-backups-by-criterion'
		response = requests.post(url, headers=self.auth_header, params={}, json={'ids': resources_ids, 'criterion': criterion})
		return response.json()

	def get_model_servers(self):
		url = self.manager + '/management/client/get-model-servers'
		response = requests.get(url, headers=self.auth_header)
		return response.json()

	def get_resources(self, criterion={}):
		url = self.manager + '/management/client/get-resources-by-criterion'
		response = requests.post(url, headers=self.auth_header, params={}, json={**criterion})
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

	def upsert_resource_backup_schedule(self, action, target_id, schedule_id, **parameters ):
		payload = {
	        "id": schedule_id,
	        "$hidden": parameters.get('hidden', False),
	        "$visibility": parameters.get('visibility', 'full'),
	        "backupType": parameters.get('backup_type', 'pln'),
	        "enabled": parameters.get('enabled', True),
	        "targetResourceId": target_id,
	        "maxBackupCount": parameters.get('max_backup_count', 1),
	        "repeatInterval": parameters.get('repeat_interval', 86400),
	        "repeatCount": parameters.get('repeat_count', 1),
	        "startTime": parameters.get('start_time', 0),
	        "endTime": parameters.get('end_time', 0),
	        "type": "resourceBackupSchedule",
	        "revision": parameters.get('revision', 0)
		}
		url = self.manager + '/management/client/'+action+'-resource-backup-schedule'
		if action == 'insert':
			response = requests.post(url, headers=self.auth_header, params={}, json={**payload})
		elif action == 'update':
			response = requests.put(url, headers=self.auth_header, params={}, json={**payload})
		return response

class BackupManager():

	def __init__(self, client):
		self.client = client



	# def start(self):
	# 	log, blob = None, None
	# 	last_date, copy_date = 0, 0
	# 	data = {
	# 		'job_updated': datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
	# 		'resources': {}
	# 	}

	# 	log = bm.get_backup_log()
	# 	if log:
	# 		blob = self.client.get_blob_content(self.client.session['id'], log['id'])
	# 		data = blob.content.decode('utf-8')
	# 		last_date = datetime.fromtimestamp(log['$modifiedDate']/1000).strftime('%Y-%m-%d-%H-%M-%S')

	# 	modified = self.get_modified(last_date)
	# 	if modified:
	# 		for m in modified:
	# 			num = data['resources'][m['id']]['num'] + 1 if m['id'] in data['resources'] else 1
	# 			data['resources'][m['id']] = {
	# 				'name': m['name'],
	# 				'last_change': last_date,
	# 				"last_backup": copy_date,
	# 				"num": num
	# 			}

	# 		write = self.write_data(json.dumps(data, indent=4), '/_LOG/job_backup.json')

	def backup(self):
		log, blob = None, None
		last_date, copy_date = 0, 0
		data = {
			'job_updated': datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
			'resources': {}
		}

		log = bm.get_backup_log()
		if log:
			blob = self.client.get_blob_content(self.client.session['id'], log['id'])
			data = blob.content.decode('utf-8')
			last_date = datetime.fromtimestamp(log['$modifiedDate']/1000).strftime('%Y-%m-%d-%H-%M-%S')

		modified = self.get_modified(last_date)
		if modified:
			for m in modified:
				backups = self.client.get_resource_backups([m['id']])
				for b in backups:
					# delete all
					delete = self.client.delete_resource_backup(m['id'], b['id'])
					print (f"Delete: {b['$resourceName']} - {b['$backupFileName']} {delete}")

	def override_schedule(self):
		resources = self.get_modified(0)
		for r in resources:
			print (r['name'])
			schedules = self.client.get_resource_backup_schedules({'$eq': { 'targetResourceId': r['id']}})
			if not schedules:
				if r['type'] == 'library:':
					insert = self.client.upsert_resource_backup_schedule(
							action = 'insert',
							target_id = r['id'],
							schedule_id = f"bimlibrary{r['id']}",
							backupType = 'bimlibrary',
							enabled = False
					)
				else:
					for key in ['bimproject', 'pln']:
						insert = self.client.upsert_resource_backup_schedule(
							action = 'insert',
							target_id = r['id'],
							schedule_id = f"{key}{r['id']}",
							backupType = key,
							enabled = False
						)
				print (f"Inserted: {r['id']} {insert}")
			else:
				for s in schedules:
					if type(s) == dict:
						update = self.client.upsert_resource_backup_schedule(
							action = 'update',
							target_id = s['targetResourceId'],
							schedule_id = s['id'],
							enabled = False
						)
						print (f"Updated: {s['id']} {update}")
					else:
						print (schedules)
			del schedules


	def get_backup_log(self):
		blobs = self.client.get_resources(
			criterion = {
				'$and': [
					{'$eq': {'type': 'blob'}},
					{'$eq': {'name': 'job_backup.log'}}
				]
			}
		)
		if blobs:
			return blobs[0]
		return None

	def get_modified(self, from_time=0):
		resources = self.client.get_resources(
			criterion = {
				'$and': [
					{'$gte': {'$modifiedDate': from_time }},
					{'$or': [
						{'$eq': {'type': 'project'}},
						{'$eq': {'type': 'library'}}
					]}
				]
			}
		)
		return resources

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
	cmd.add_argument('-b', '--backup_override', required=False, help='Override Backups')
	arg = cmd.parse_args()

	try:
		bc = BIMcloud(**vars(arg))
		bm = BackupManager(bc)

		if arg.backup_override == 'y':
			bo = bm.override_schedule()

		# backup = bm.backup()



	except:
		raise