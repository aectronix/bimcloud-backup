import argparse
import gc
import logging
import math
import sys
import time

from datetime import date, datetime, timedelta
from src import *

class BackupManager():

	def __init__(self, client, storage, **parameters):
		self.log = logging.getLogger('BackupManager')
		self.client = client
		self.storage = storage
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
		self.log.error(f"Process timed out! Skipped. ({fn.__name__} {args})")
		return None

	def backup(self, ids=[]) -> None:
		"""	Starts resource backup procedure. """
		i = 0
		resources = self.get_resources(ids)
		if resources:
			self.log.info(f"Found resources: {len(resources)}, starting backup process...")
			for resource in resources:
				i += 1
				self.log.info(f"Resource #{i}:")
				self.log.info(f"{resource['id']} ({resource['type']}: \"{resource['name']}\", {round(resource['$size']/1024 **2, 2)} Mb)")
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

					if resource['type'] == 'project':
						for b in backups:
							if b and b.get('$time') <= resource['$modifiedDate']:
								delete_backup_r = self.delete_project_backup(resource['id'], b['id'])
								self.log.info(f"Deleted: {len(backups)} backups, {delete_backup_r}")
						project_create_r = self.create_project_backup(resource['id'])
						result = self.run_with_timeout(self.is_project_backup_created, timeout, 1, project_create_r['id'])
						backup_new = self.is_project_backup_valid(result, start_time)
						if backup_new:
							self.log.info(f"Backup successfully created.")
							self.transfer_backup(resource['name'], resource['id'], resource['$size'], backup_new['id'])

					if resource['type'] == 'library':
						library_invoke_r = self.invoke_library_backup(resource['id'], start_time )
						result = self.run_with_timeout(self.is_library_backup_created, timeout, 1, resource['id'], start_time)
						schedule_delete_r = self.delete_resource_schedules(resource['id'])
						backup_new = self.is_library_backup_valid(resource['id'], result['id'], start_time)
						if backup_new:
							self.log.info(f"Backup successfully created.")
							self.transfer_backup(resource['name'], resource['id'], resource['$size'], backup_new['id'])
				else:
					self.log.info(f"Resource has valid backup, skipped")
				# don't hurry up
				time.sleep(1)

				del resource
				gc.collect()

				# self.client._create_or_refresh_auth_token()
				# self.client._create_or_refresh_session()

	def get_resources(self, ids: str):
		"""	Retrieves resources from bimcloud storage. """
		params = { 'sort-by': '$time', 'sort-direction': 'desc' }
		if ids:
			result = self.client.get_resources_by_id_list([ids], params)
			if result:
				return result
			self.log.info(f"Resource not found.")
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
		if jobs and not isinstance(jobs, str):
			job = jobs.json()[0]
			self.log.info(
			    f"> {job['status']}: {job['progress']['current']}/{job['progress']['max']}, (runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec)<rf>"
			)
			if job['status'] in ['completed', 'failed']:
				print ('', flush=True)
				return job
		return None

	def is_project_backup_valid(self, job, start_time):
		"""	Validates created backup by checking it's existing & props. """
		if job:
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
				if backup.get('$statusId') == '_server.backup.status.done' and backup.get('$fileSize', 0) > 0:
					return backup
		return None

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
		self.log.error(f"Failed to insert backup schedule: {resource_id}")
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
			if backup.get('id') == backup_id and backup.get('$statusId') == '_server.backup.status.done' and backup.get('$fileSize', 0) > 0:
				return backup
		return False

	def delete_resource_schedules(self, resource_id):
		schedule_delete_r = None
		schedules = self.client.get_resource_backup_schedules({'$eq': {'targetResourceId': resource_id}})
		if schedules:
			print (schedules)
			for s in schedules:
				print (s)
				if s and not isinstance(s, str):
					schedule_delete_r = self.client.delete_resource_backup_schedule(s['id'])
			self.log.info(f"Deleted: {len(schedules)} schedules")
			if schedule_delete_r:
				return schedule_delete_r
			return None

	def get_backup_data(self, resource_id, backup_id, timeout=300):
		with self.client.download_backup(resource_id, backup_id, timeout=timeout, stream=True) as response:
			response.raise_for_status()
			total_length = response.headers.get('content-length')
			if total_length is not None:
				total_length = int(total_length)
			downloaded = 0
			chunks = []
			start_time = time.time()
			last_update = start_time

			for chunk in response.iter_content(chunk_size=4096):
				if chunk:
					chunks.append(chunk)
					downloaded += len(chunk)
					now = time.time()
					runtime = now - start_time
					if runtime > timeout:
						self.log.error(f"timeout!")
						return None
					if now - last_update >= 1:
						self.log.info(f"> receiving {round(downloaded/total_length*100)}%, runtime: {round(runtime)}/{round(timeout)} sec<rf>")
						last_update = now
			content = b''.join(chunks)
			self.log.info(f"> received {round(downloaded/total_length*100)}%, runtime: {round(runtime)}/{round(timeout)} sec<rf>")
			print ('', flush=True)
		return content

	def transfer_backup(self, resource_name, resource_id, resource_size, backup_id):
		self.log.info(f"Get contents and save to the cloud...")
		timeout = self.get_timeout_from_filesize(resource_size, e=1.30) # adjusting for google
		# data = self.isolate_with_timeout(self.get_backup_data, timeout=timeout, delay=1, message='receiving', resource_id=resource_id, backup_id=backup_id)
		data = self.get_backup_data(resource_id, backup_id, timeout)
		if not data:
			logger.error(f"Failed to retreive backup data! Skipped.")
			return None
		files = self.storage.get_folder_resources('1XKPjCnJJUunDn67wMgcQUoYargTmrOJ0')
		match_file = next((f for f in files if f['name'] == resource_name+'.BIMProject25'), None)
		match_file_id = match_file['id'] if match_file else None
		request = self.storage.prepare_upload(
			data,
			file_name = resource_name+'.BIMProject25',
			file_id = match_file_id,
			resource_id = resource_id
		)
		upload = self.run_with_timeout(self.storage.upload_chunks, timeout, 0.05, request)
		if upload:
			self.log.info(f"Successfully uploaded to the cloud. ({upload['id']})")

		del data
		gc.collect()


if __name__ == "__main__":

	start_time = time.time()

	cmd = argparse.ArgumentParser()
	# cloud
	cmd.add_argument('-m', '--manager', required=True, help='URL of the BIMcloud Manager')
	cmd.add_argument('-c', '--client', required=True, help='Client Identification')
	cmd.add_argument('-u', '--user', required=True, help='User Login')
	cmd.add_argument('-p', '--password', required=True, help='User Password')
	cmd.add_argument('-s', '--schedule_enabled', choices=['y', 'n'], default='n', help='Enable default schedules')
	cmd.add_argument('-f', '--filepath', required=False, help='Path to the log file')
	cmd.add_argument('-r', '--resource', required=False, help='Resource Id')
	# drive
	cmd.add_argument('-k', '--gd_cred_path', required=True, help='Path to Gogole credentials')
	arg = cmd.parse_args()

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

	logger = logging.getLogger('BackupManager')
	logger.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d.%M.%y %H:%M:%S')

	handler_console = LogHandler(logging.StreamHandler(sys.stdout))
	handler_console.setFormatter(formatter)
	handler_console.setLevel(logging.INFO)

	handler_file = LogHandler
	handler_file = logging.FileHandler(arg.filepath, mode='a')
	handler_file.setFormatter(formatter)
	handler_file.setLevel(logging.WARNING)

	logger.addHandler(handler_console)
	logger.addHandler(handler_file)

	try:
		cloud = BIMcloudAPI(**vars(arg))
		drive = GoogleDriveAPI(arg.gd_cred_path, arg.client)
		if cloud and drive:
			manager = BackupManager(cloud, drive, schedule_enabled = arg.schedule_enabled)
			backup = manager.backup(arg.resource)
			# cloud.test()

	except Exception as e:
		logger.error(f"Unexpected error: {e}", exc_info=True)
		sys.exit(1)
	finally:
		close = cloud.close_session()
		logger.info(f"Closing session... {close.reason}")
		logger.info(f"Finished in {round(time.time()-start_time)} sec")