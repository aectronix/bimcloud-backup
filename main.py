import argparse
import json
import math
import requests
import time

from datetime import date, datetime, timedelta

DELTA = 10 - 3600
BACKUP_SCHEDULE = {
    'id': None,
    '$hidden': False,
    "$visibility": 'full',
    'backupType': None,
    'enabled': True,
    'targetResourceId': None,
    'maxBackupCount': 1,
    'repeatInterval': 3600,
    'repeatCount': 0,
    'startTime': 0,
    'endTime': 0,
    'type': 'resourceBackupSchedule',
    'revision': 0,
}

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
			print(f"Connected to {self.manager}")
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

	def insert_resource_backup_schedule(self, data):
		url = self.manager + '/management/client/insert-resource-backup-schedule'
		response = requests.post(url, headers=self.auth_header, json=data)
		return response


class BackupManager():

	def __init__(self, client):
		print (f"Initializing backup manager with {type(client)}")
		self.client = client

	@staticmethod
	def get_timeout_from_filesize(size):
		"""	Calculates timeout while processing the file regarding it's size in bytes.
			Starting timeout defined by t; curve steepness defined by s.
		"""
		t, s, base = 0.85, 1.0065, 60
		return base + t * (s ** (size/1000000))

	def backup(self) -> None:
		"""	Starts resource backup procedure.
		"""
		print (f"Starting backup routine...")
		criterion = {
			'$or': [
				# {'$eq': {'id': '9469F25B-D6DD-4CC3-8026-B85AC8338A16'}},
				{'$eq': {'id': '8E539125-25D2-48F7-AE49-755D6AB6E293'}}
			]
		}
		resources = self.client.get_resources(criterion)
		for resource in resources:
			print (f"Resource: {resource['id']} (\"{resource['name']}\")")
			has_outdated_backup = True
			backups = self.client.get_resource_backups([resource['id']], params={'sort-by': '$time', 'sort-direction': 'desc'}) or []
			# check backups
			if backups and backups[0].get('$time') >= resource['$modifiedDate']:
				has_outdated_backup = False
			# create new, remove old
			if has_outdated_backup:
				start_time = time.time()

				if resource['type'] == 'project':
					for b in backups:
						if b and b.get('$time') <= resource['$modifiedDate']:
							project_delete_r = self.delete_project_backup(resource['id'], b['id'])
							print (f"Delete: {b['id']} {project_delete_r}")
					project_create_r = self.create_project_backup(resource['id'])
					is_valid = self.validate_project_backup(project_create_r, start_time)
					if is_valid:
						print ('OK')

				if resource['type'] == 'library':
					library_create_r = self.create_library_backup(
						resource,
						backupType = 'bimlibrary',
						maxBackupCount = 1,
						repeatInterval = 3600,
						startTime = start_time + DELTA
					)
					is_valid = self.validate_library_backup(resource['id'], library_create_r, start_time*1000)
					if is_valid:
						print ('OK')

			# don't hurry up
			time.sleep(1)

	def create_project_backup(self, resource):
		"""	Creates a new backup for project resource.
		Args:
			resource_id (str): Resource id
		Returns:
			bool: True if process succeded
		"""
		print (f"Creating a new backup")
		response, job = None, None
		response = self.client.create_resource_backup(resource['id'], 'bimproject', 'Scripted Backup')
		if response and response.get('id'):
			job = response
			while job['status'] not in ['completed', 'failed']:
				print (f"> {job['status']}: {job['progress']['current']} / {job['progress']['max']} ",  end='\r')
				job = self.client.get_jobs(
					criterion = {
						'$and': [
							{'$eq': {'jobType': 'createProjectBackup'}},
							{'$eq': {'id': response['id']}}
						]
					}
				)[0]
				time.sleep(2)

		if job['status'] == 'completed':
			print ('')
			return job
		return None

	def validate_project_backup(self, job, start_time):
		"""	Validates created backup by checking it's existing & props.
		Args:
			job (dict): Job dictionary object, after creation is launched
			start_time (int): Timestamp of backup creation
		Returns:
			bool: True if validation succeded
		"""
		if job:
			resource_id = next(filter(lambda x: x['name'] == 'projectId', job['properties']), {}).get('value')
			backup = self.client.get_resource_backups(
				[resource_id],
				criterion = {
					'$and': [
						{'$eq': {'$resourceId': resource_id}},
						{'$gte': {'$time': start_time}}
					]
				},
				params = {
					'sort-by': '$time',
					'sort-direction': 'desc'
				}
			)[0] or None
			if backup and \
				backup.get('$statusId') == '_server.backup.status.done' and \
				backup.get('$fileSize') > 0:
				return True
		return False

	def delete_project_backup(self, resource_id, backup_id):
		"""	Removes targeted backup.
		Args:
			resource_id (str): Resource (project) id
			backup_id (str): Backup id
		Returns:
			bool: True if process succeded
		"""
		response = self.client.delete_resource_backup(resource_id, backup_id)
		return response

	def create_library_backup(self, resource, **parameters):
		print (f"Inserting temporary backup schedule to trigger auto backup")
		timeout = self.get_timeout_from_filesize(resource['$size'])
		schedule = BACKUP_SCHEDULE
		schedule['id'] = 'bimlibrary'+resource['id']
		schedule['targetResourceId'] = resource['id']
		for key, value in parameters.items():
			if key in schedule:
				schedule[key] = value
		response = self.client.insert_resource_backup_schedule(schedule)
		if response:
			print (f"Inserted, awaiting scheduler to create backup")
			backup = None
			start_time = (schedule['startTime'] - DELTA) * 1000
			spent_time = 0
			while not backup or backup.get('$time') < start_time:
				print (f"> Waiting for creation, time passed: {spent_time} / {round(timeout)}",  end='\r', flush=True)
				if spent_time >= timeout:
					print (f"\nTimeout exceeded!")
					break
				spent_time += 1
				response = self.client.get_resource_backups(
					[resource['id']],
					criterion = {
						'$and': [
							{'$eq': {'$resourceId': resource['id']}},
							{'$eq': {'$formatId': '_server.backup.format.bimlibrary-automatic'}},
							{'$gte': {'$time': start_time}}
						]
					},
					params = {
						'sort-by': '$time',
						'sort-direction': 'desc'
					}
				)
				if response:
					backup = response[0]
				time.sleep(1)
		# remove schedule
		schedule_delete = self.client.delete_resource_backup_schedule('bimlibrary'+resource['id'])
		print (f"Reset schedule: {schedule_delete}")
		# finalize
		if backup and backup.get('$time') >= start_time:
			print ('Backup created')
			return backup['id']
		return None

	def validate_library_backup(self, resource_id, backup_id, start_time):
		response = self.client.get_resource_backups(
			[resource_id],
			criterion = {
				'$and': [
					{'$eq': {'$resourceId': resource_id}},
					{'$eq': {'$formatId': '_server.backup.format.bimlibrary-automatic'}},
					{'$gte': {'$time': start_time}}
				]
			},
			params = {
				'sort-by': '$time',
				'sort-direction': 'desc'
			}
		)
		backup = response[0] if response else None
		if backup and \
			backup.get('id') == backup_id and \
			backup.get('$statusId') == '_server.backup.status.done' and \
			backup.get('$fileSize') > 0:
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