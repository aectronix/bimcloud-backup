import argparse
import json
import math
import requests
import time

from datetime import date, datetime, timedelta

DELTA = 15 - 3600
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

class BackupManager():

	def __init__(self, client, **parameters):
		print (f"Initializing backup manager for {client.manager}...")
		self.client = client
		self.schedule_enabled = parameters.get('schedule_enabled')

	@staticmethod
	def get_timeout_from_filesize(size, b=60.0, f=15.0, e=1.45, div=1000000):
		"""	Calculates timeout while processing the file regarding it's size in bytes.
		Args:
			b (int): 	base time, seconds
			f (int): 	scaling factor
			e (int): 	exponent power
			div (int):	division, bytes (to get Kb, Mb, Gb etc as input factor)
		"""
		return b + round(f * (size/div ** e))

	def backup(self, ids=[]) -> None:
		"""	Starts resource backup procedure.
		"""
		i = 0
		resources = self.get_resources(ids)
		print (f"Found: {len(resources)}, starting backup process...")
		for resource in resources:
			i += 1
			print (f"Resource #{i}: {resource['id']} ({resource['type']}: \"{resource['name']}\")")
			# remove all schedules if required
			schedules = self.client.get_resource_backup_schedules({'$eq': {'targetResourceId': resource['id']}})
			if schedules and self.schedule_enabled == 'n':
				for s in schedules:
					schedule_delete_r = self.client.delete_resource_backup_schedule(s['id'])
				print (f"Deleted: {len(schedules)} backup schedules, {schedule_delete_r}")

			# check backups
			has_outdated_backup = True
			backups = self.client.get_resource_backups([resource['id']], params={'sort-by': '$time', 'sort-direction': 'desc'}) or []
			if 	(backups and backups[0].get('$time') >= resource['$modifiedDate']) or \
				(not backups and resource['$modifiedDate'] == resource['$uploadedTime']):
				has_outdated_backup = False

			# create new, remove old
			if has_outdated_backup:
				start_time = time.time()

				if resource['type'] == 'project':
					for b in backups:
						if b and b.get('$time') <= resource['$modifiedDate']:
							project_delete_r = self.delete_project_backup(resource['id'], b['id'])
							print (f"Delete: {b['id']} {project_delete_r}")
					project_create_r = self.create_project_backup(resource)
					is_valid = self.validate_project_backup(project_create_r, start_time)
					if is_valid:
						print ('OK')

				if resource['type'] == 'library':
					# if backups and backups[0]['revision'] > resource['modifiedDate']
					library_create_r = self.create_library_backup(resource, startTime = start_time + DELTA )
					is_valid = self.validate_library_backup(resource['id'], library_create_r, start_time*1000)
					if is_valid:
						print ('OK')
			else:
				print (f"Resource has valid backup, skipped")
			# don't hurry up
			time.sleep(1)

	def get_resources(self, ids=[]):
		params = { 'sort-by': '$time', 'sort-direction': 'desc' }
		if ids:
			return self.client.get_resources_by_id_list(ids, params)
		return self.client.get_resources_by_criterion(
			{
				'$or': [
					{'$eq': {'type': 'project'}},
					{'$eq': {'type': 'library'}},
				]
			},
			params
		)


	def create_project_backup(self, resource):
		"""	Creates a new backup for project resource.
		Args:
			resource_id (str): Resource id
		Returns:
			bool: True if process succeded
		"""
		print (f"Creating a new backup")
		start_time = time.time()
		timeout = self.get_timeout_from_filesize(resource['$size'])
		response, job = None, None
		response = self.client.create_resource_backup(
			resource['id'],
			'bimproject',
			'Scripted Backup'
		)
		if response and response.get('id'):
			job = response
			while job['status'] not in ['completed', 'failed']:
				spent_time = round(time.time() - start_time)
				print (f"> {job['status']}: {job['progress']['current']} / {job['progress']['max']} time passed: {spent_time} / {round(timeout)}\t",  end='\r')
				if spent_time >= timeout:
					print (f"\nTimeout exceeded!")
					break
				job = self.client.get_jobs(
					criterion = {
						'$and': [
							{'$eq': {'jobType': 'createProjectBackup'}},
							{'$eq': {'id': response['id']}}
						]
					}
				)[0]
				time.sleep(1)

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
		response = self.client.insert_resource_backup_schedule(
			targetResourceId = resource['id'],
			backupType = 'bimlibrary',
			maxBackupCount = 1,
			repeatInterval = 3600,
			startTime = parameters.get('startTime')
		)
		if response:
			print (f"Inserted, awaiting scheduler to create backup")
			plan_time = (parameters.get('startTime') - DELTA) * 1000
			start_time = time.time()
			backup = None
			while not backup or backup.get('$time') < plan_time:
				spent_time = round(time.time() - start_time)
				print (f"> Waiting for creation, time passed: {spent_time} / {round(timeout)}\t",  end='\r', flush=True)
				if spent_time >= timeout:
					print (f"\nTimeout exceeded!")
					break
				response = self.client.get_resource_backups(
					[resource['id']],
					criterion = {
						'$and': [
							{'$eq': {'$resourceId': resource['id']}},
							{'$eq': {'$formatId': '_server.backup.format.bimlibrary-automatic'}},
							{'$gte': {'$time': plan_time}}
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
		print (f"")
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


if __name__ == "__main__":

	start_time = time.time()

	cmd = argparse.ArgumentParser()
	cmd.add_argument('-m', '--manager', required=True, help='URL of the BIMcloud Manager')
	cmd.add_argument('-c', '--client', required=True, help='Client Identification')
	cmd.add_argument('-u', '--user', required=True, help='User Login')
	cmd.add_argument('-p', '--password', required=True, help='User Password')
	cmd.add_argument('-s', '--schedule_enabled', required=False, help='Enable default schedules')
	arg = cmd.parse_args()

	try:
		cloud = BIMcloud(**vars(arg))
		manager = BackupManager(
			cloud,
			schedule_enabled = arg.schedule_enabled
		)

		bcp = manager.backup(['4292A87F-BD20-4D54-B377-55F93D3AE202'])

		# print (bm.get_timeout_from_filesize(1000*1000*1000*3))

	except:
		raise

	print (f"Completed in {round(time.time()-start_time)} sec")