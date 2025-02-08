import argparse
import json
import logging
import math
import requests
import sys
import time

from datetime import date, datetime, timedelta

class BIMcloud():

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
			self.log.error(f"Unexpected error: {e}", exc_info=True)
			sys.exit(1)

	def _grant(self):
		try:
			servers = self.get_model_servers()
			ticket = self.get_ticket(servers[0]['id'])
			session = self.create_session(self.user, ticket)
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

class LogHandler(logging.StreamHandler):
	""" Custom log handler to overwrite some output methods. """
	def emit(self, record):
		try:
			msg = self.format(record)
			if msg.endswith('<rf>'):
				msg = msg.replace('<rf>', '')
				sys.stdout.write(f"\r{msg}".ljust(120) + '\r')
				sys.stdout.flush()
			else:
				sys.stdout.write(f"{msg}\n")
		except Exception:
			self.handleError(record)

class BackupManager():

	def __init__(self, client, **parameters):
		self.log = logging.getLogger('BackupManager')
		self.log.info(f"Initializing backup manager for {client.manager}")
		self.client = client
		self.schedule_enabled = parameters.get('schedule_enabled')

	@staticmethod
	def get_timeout_from_filesize(size, b=60.0, f=15.0, e=1.45, div=1000000) -> int:
		"""	Calculates timeout while processing the file regarding it's size in bytes.
		Args:
			b (int): 	base time, seconds
			f (int): 	scaling factor
			e (int): 	exponent power
			div (int):	division, bytes (to get Kb, Mb, Gb etc as input factor)
		"""
		return b + round(f * (size/div ** e), 0)

	def run_with_timeout(self, fn, timeout, delay, *args, **kwargs):
		"""	Adds timeout for the function. """
		start_time = time.time()
		while (runtime := time.time() - start_time) < timeout:
			kwargs.update({"runtime": runtime, "timeout": timeout})
			if result := fn(*args, **kwargs):
				return result
			time.sleep(delay)
		print ('', flush=True)
		self.log.error(f"Process timed out! Skipped.")
		return None

	def backup(self, ids=[]) -> None:
		"""	Starts resource backup procedure. """
		i = 0
		resources = self.get_resources(ids)
		self.log.info(f"Resources: {len(resources)}, starting backup process...")

		for resource in resources:
			i += 1
			self.log.info(f"Resource #{i}:")
			self.log.info(f"{resource['id']} ({resource['type']}: \"{resource['name']}\")")
			timeout = self.get_timeout_from_filesize(resource['$size'])
			# remove all schedules if required
			if self.schedule_enabled == 'n':
				schedule_delete_r = self.delete_resource_schedules(resource['id'])
			# check backups
			has_outdated_backup = True
			backups = self.client.get_resource_backups([resource['id']], params={'sort-by': '$time', 'sort-direction': 'desc'}) or []
			if 	(backups and backups[0].get('$time') >= resource.get('$modifiedDate')) or \
				(not backups and resource.get('$modifiedDate') == resource.get('$uploadedTime')): # special for libs
				has_outdated_backup = False
			# create new, remove old
			if has_outdated_backup:
				start_time = time.time()
				# prj
				if resource['type'] == 'project':
					for b in backups:
						if b and b.get('$time') <= resource['$modifiedDate']:
							delete_backup_r = self.delete_project_backup(resource['id'], b['id'])
							self.log.info(f"Deleted: {len(backups)} backups, {delete_backup_r}")
					project_create_r = self.create_project_backup(resource['id'])
					result = self.run_with_timeout(self.is_project_backup_created, timeout, 1, project_create_r['id'])
					if result and self.is_project_backup_valid(result, start_time):
						self.log.info(f"Backup successfully created.")
				# lib
				if resource['type'] == 'library':
					library_invoke_r = self.invoke_library_backup(resource['id'], start_time )
					result = self.run_with_timeout(self.is_library_backup_created, timeout, 1, resource['id'], start_time)
					schedule_delete_r = self.delete_resource_schedules(resource['id'])
					if result and self.is_library_backup_valid(resource['id'], result['id'], start_time):
						self.log.info(f"Backup successfully created.")
			else:
				self.log.info(f"Resource has valid backup, skipped")
			# don't hurry up
			time.sleep(1)

	def get_resources(self, ids=[]):
		"""	Retrieves resources from bimcloud storage. """
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

	def create_project_backup(self, resource_id):
		"""	Creates a new backup for project resource. """
		self.log.info(f"Creating a new backup...")
		response = self.client.create_resource_backup(
			resource_id,
			'bimproject',
			'Scripted Backup'
		)
		if not response or not response.get('id'):
			self.log.error(f"Failed to initiate backup.")
			return None
		return response

	def is_project_backup_created(self, job_id, **kwargs):
		"""	Checks backup completion status. """
		jobs = self.client.get_jobs(
			criterion={
				'$and': [
					{'$eq': {'jobType': 'createProjectBackup'}},
					{'$eq': {'id': job_id}}
				]
			},
			params = {
				'sort-by': '$time',
				'sort-direction': 'desc'
			}
		)
		if jobs:
			job = jobs[0]
			self.log.info(
			    f"> {job['status']}: {job['progress']['current']}/{job['progress']['max']}, "
			    f"(runtime: {round(kwargs.get('runtime'), 0)}/{round(kwargs.get('timeout'), 0)} sec)<rf>"
			)
			if job['status'] in ['completed', 'failed']:
				print ('', flush=True)
				return job
		return None

	def is_project_backup_valid(self, job, start_time):
		"""	Validates created backup by checking it's existing & props. """
		if job:
			# resource_id = next((x['value'] for x in job.get('properties', []) if x.get('name') == 'projectId'), None)
			resource_id = next((x.get('value') for x in (job.get('properties') or []) if x.get('name') == 'projectId'), None)
			backups = self.client.get_resource_backups(
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
			)
			if backups:
				backup = backups[0]
				return (
					backup.get('$statusId') == '_server.backup.status.done' and
					backup.get('$fileSize', 0) > 0
				)
		return False

	def delete_project_backup(self, resource_id, backup_id):
		"""	Removes targeted backup. """
		response = self.client.delete_resource_backup(resource_id, backup_id)
		return response

	def invoke_library_backup(self, resource_id, action_time, offset=10, interval=3600):
		""" Triggers scheduler to trigger auto backup.
			Note: workaround
			As we cannot remove custom backups from the library resource later, we're forced
			to operate with automatic backups in a single copy only. To create an automatic
			backup we could setup a scheduler with the start time a 1h before and small delay.
		"""
		self.log.info(f"Inserting temporary backup schedule to trigger auto backup...")
		response = self.client.insert_resource_backup_schedule(
			targetResourceId = resource_id,
			backupType = 'bimlibrary',
			maxBackupCount = 1,
			repeatInterval = 3600,
			startTime = action_time + offset - interval
		)
		if response:
			self.log.info(f"Inserted, expecting scheduler to create backup")
			return response
		return None

	def is_library_backup_created(self, resource_id, action_time, **kwargs):
		backups = self.client.get_resource_backups(
			[resource_id],
			criterion = {
				'$and': [
					{'$eq': {'$resourceId': resource_id}},
					{'$eq': {'$formatId': '_server.backup.format.bimlibrary-automatic'}},
					{'$gte': {'$time': action_time*1000}} # ensure that it's exactly ours
				]
			},
			params = {
				'sort-by': '$time',
				'sort-direction': 'desc'
			}
		)
		self.log.info(f"> awaiting auto backup, runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec<rf>")
		if backups:
			backup = backups[0]
			# return backup if backup.get('$time') >= action_time else None
			if backup.get('$time') >= action_time:
				print ('', flush=True)
				return backup
		return None

	def is_library_backup_valid(self, resource_id, backup_id, start_time):
		backups = self.client.get_resource_backups(
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
		if backups:
			backup = backups[0]
			return (
				backup.get('id') == backup_id and
				backup.get('$statusId') == '_server.backup.status.done' and
				backup.get('$fileSize', 0) > 0
			)
		return False

	def delete_resource_schedules(self, resource_id):
		schedules = self.client.get_resource_backup_schedules({'$eq': {'targetResourceId': resource_id}})
		if schedules:
			for s in schedules:
				schedule_delete_r = self.client.delete_resource_backup_schedule(s['id'])
			self.log.info(f"Deleted: {len(schedules)} schedules, {schedule_delete_r}")
			return schedule_delete_r


if __name__ == "__main__":

	start_time = time.time()

	logger = logging.getLogger('BackupManager')
	logger.setLevel(logging.INFO)
	handler = LogHandler()
	formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d.%M.%y %H:%M:%S')
	handler.setFormatter(formatter)
	logger.addHandler(handler)

	cmd = argparse.ArgumentParser()
	cmd.add_argument('-m', '--manager', required=True, help='URL of the BIMcloud Manager')
	cmd.add_argument('-c', '--client', required=True, help='Client Identification')
	cmd.add_argument('-u', '--user', required=True, help='User Login')
	cmd.add_argument('-p', '--password', required=True, help='User Password')
	cmd.add_argument('-s', '--schedule_enabled', choices=['y', 'n'], default='n', help='Enable default schedules')
	arg = cmd.parse_args()

	try:
		cloud = BIMcloud(**vars(arg))
		if cloud:
			manager = BackupManager(cloud, schedule_enabled = arg.schedule_enabled)
			backup = manager.backup() # all
	except Exception as e:
		logger.error(f"Unexpected error: {e}", exc_info=True)
		sys.exit(1)
	finally:
		logger.info(f"Finished in {round(time.time()-start_time)} sec")