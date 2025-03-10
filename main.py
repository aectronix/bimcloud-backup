import argparse
import logging
import math
import sys
import time

from src import BIMcloudAPI, GoogleDriveAPI, NotionAPI


def setup(arg):
	"""
	Configure global settings (i.e. logging, etc).

	Args:
		arg: An object with configuration attributes, e.g., arg.filepath.
	"""
	logger = logging.getLogger('BackupManager')
	logger.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d.%M.%y %H:%M:%S')

	handler_console = LogHandler(logging.StreamHandler(sys.stdout))
	handler_console.setFormatter(formatter)
	handler_console.setLevel(logging.INFO)

	handler_file = logging.FileHandler(arg.filepath, mode='a')
	handler_file.setFormatter(formatter)
	handler_file.setLevel(logging.WARNING)

	logger.addHandler(handler_console)
	logger.addHandler(handler_file)

class LogHandler(logging.StreamHandler):
	"""
	Custom log handler that supports inline updates using carriage returns.
	When a log message ends with '<rf>', it will update the same line.
	"""
	def emit(self, record):
		try:
			msg = self.format(record)
			# If the message ends with the special marker, update inline.
			if msg.endswith('<rf>'):
				msg = msg.replace('<rf>', '')
				sys.stdout.write(f"\r{msg.ljust(120)}\r")
				sys.stdout.flush()
			else:
				sys.stdout.write(f"{msg}\n")
		except Exception:
			self.handleError(record)

class BackupManager():

	def __init__(self, client, storage, **kwargs):
		"""
		Initialize the BackupManager.

		Args:
			client: BIMcloud API client instance.
			storage: Google Drive API instance.
			**kwargs: Additional parameters
		"""
		self.log = logging.getLogger('BackupManager')
		self.client = client
		self.storage = storage

		self.report = {
			'backups': 0,
			'endtime': 0,
			'errors': 0
		}

	@staticmethod
	def get_timeout_from_filesize(size, b=60.0, f=15.0, e=1.40, div=1000000) -> int:
		"""
		Calculate a timeout based on the file size.

		Args:
			size (int): File size in bytes.
			b (float): Base time in seconds.
			f (float): Scaling factor.
			e (float): Exponent.
			div (int): Divisor to convert bytes (e.g. to MB).

		Returns:
			int: Calculated timeout in seconds.
		"""
		return b + round(f * (size/div ** e), 0)

	def run_with_timeout(self, fn, timeout, delay, *args, **kwargs):
		"""
		Execute a function repeatedly until it returns a result or the timeout expires.

		Args:
			fn (callable): The function to execute.
			timeout (int): Maximum time in seconds to wait.
			delay (int): Delay between function calls.
			*args: Positional arguments for fn.
			**kwargs: Keyword arguments for fn.

		Returns:
			The result returned by fn, or None if timed out.
		"""
		start_time = time.time()
		while (runtime := time.time() - start_time) < timeout:
			kwargs.update({"runtime": runtime, "timeout": timeout})
			if result := fn(*args, **kwargs):
				return result
			time.sleep(delay)
		print ('', flush=True)
		self.log.error(f"Process timed out! Skipped. ({fn.__name__} {args})")
		self.report['errors'] += 1
		return None

	def backup(self, ids=[]) -> None:
		"""
		Start the resource backup procedure.
		Iterates over resources and processes them based on type.
		"""
		resources = self.get_resources(ids)
		if not resources:
			self.log.info("No resources found.")
			self.report['errors'] += 1
			return

		self.log.info(f"Found resources: {len(resources)}, starting backup process...")
		i, backups_created = 0, 0
		for resource in resources:
			i += 1
			self.log.info(f"Resource #{i}:")
			self.log.info(f"{resource['id']} ({resource['type']}: \"{resource['name']}\", {round(resource['$size']/1024 **2, 2)} Mb)")
			timeout = self.get_timeout_from_filesize(resource['$size'])
			
			# remove all schedules if required
			_ = self.delete_resource_schedules(resource['id'])

			# check backups
			has_outdated_backup = True
			backups = self.client.get_resource_backups([resource['id']], params={'sort-by': '$time', 'sort-direction': 'desc'}) or []
			if 	(backups and backups[0].get('$time') >= resource.get('$modifiedDate')) or \
				(not backups and resource.get('$modifiedDate') == resource.get('$uploadedTime')): # special for libs
				has_outdated_backup = False

			# create new, remove old
			if has_outdated_backup:
				start_time = time.time()
				backup_new = None

				if resource['type'] == 'project':
					for bcp in backups:
						if bcp and bcp.get('$time') <= resource['$modifiedDate']:
							delete_backup_r = self.delete_project_backup(resource['id'], bcp['id'])
							self.log.info(f"Deleted: {len(backups)} backups, {delete_backup_r}")
					project_create_r = self.create_project_backup(resource['id'])
					result = self.run_with_timeout(self.is_project_backup_created, timeout, 1, project_create_r['id'])
					backup_new = self.is_project_backup_valid(result, start_time)

				if resource['type'] == 'library':
					library_invoke_r = self.invoke_library_backup(resource['id'], start_time )
					result = self.run_with_timeout(self.is_library_backup_created, timeout, 1, resource['id'], start_time)
					schedule_delete_r = self.delete_resource_schedules(resource['id'])
					backup_new = self.is_library_backup_valid(resource['id'], result['id'], start_time)

				if backup_new:
					self.log.info(f"Backup successfully created.")
					self.transfer_backup(resource, backup_new['id'])
					backups_created += 1
			else:
				self.log.info(f"Resource has valid backup, skipped")

			# don't hurry up
			time.sleep(1)
			del resource

		self.report['backups'] = backups_created
		self.report['endtime'] = time.time()

	def get_resources(self, ids: str):
		"""	Retrieves resources from bimcloud storage. """
		params = { 'sort-by': '$time', 'sort-direction': 'desc' }
		if ids:
			result = self.client.get_resources_by_id_list([ids], params)
			if result:
				return result
			return None
		return self.client.get_resources_by_criterion(
			{
				'$or': [
					{'$eq': {'type': 'project'}},
					{'$eq': {'type': 'library'}},
				]
			},
			params
		)

	def create_project_backup(self, resource_id: str):
		"""	Creates a new backup for project resource. """
		self.log.info(f"Creating a new backup...")
		response = self.client.create_resource_backup(
			resource_id,
			'bimproject',
			'Scripted Backup'
		)
		if not response or not response.get('id'):
			self.log.error(f"Failed to initiate backup.")
			self.report['errors'] += 1
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
			job = jobs[0]
			self.log.info(f"> {job['status']}: {job['progress']['current']}/{job['progress']['max']}, (runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec)<rf>")
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
		"""
		Delete a specific project backup.

		Args:
			resource_id (str): The resource ID.
			backup_id (str): The backup ID.

		Returns:
			The deletion response.
		"""
		response = self.client.delete_resource_backup(resource_id, backup_id)
		return response

	def invoke_library_backup(self, resource_id, action_time, offset=10, interval=3600):
		"""
		Trigger the scheduler to create an automatic library backup.
		Note: Workaround to force a single automatic backup copy.

		Args:
			resource_id (str): The resource ID.
			action_time (float): The action timestamp.
			offset (int, optional): Offset for start time.
			interval (int, optional): The interval between backups.

		Returns:
			None
		"""
		self.log.info(f"Inserting temporary backup schedule to trigger an auto backup...")
		try:
			_ = self.client.insert_resource_backup_schedule(
				targetResourceId = resource_id,
				backupType = 'bimlibrary',
				maxBackupCount = 1,
				repeatInterval = 3600,
				startTime = action_time + offset - interval
			)
		except Exception as e:
			self.log.error(f"Response error: {e}", exc_info=True)
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
		""" Validate a library backup by comparing its properties. """
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
			if backup.get('id') == backup_id and backup.get('$statusId') == '_server.backup.status.done' and backup.get('$fileSize', 0) > 0:
				return backup
		return False

	def delete_resource_schedules(self, resource_id: str):
		""" Delete backup schedules for a specific resource. """
		schedule_delete_r = None
		schedules = self.client.get_resource_backup_schedules({'$eq': {'targetResourceId': resource_id}})
		if schedules:
			for s in schedules:
				if s and not isinstance(s, str):
					schedule_delete_r = self.client.delete_resource_backup_schedule(s['id'])
			self.log.info(f"Deleted: {len(schedules)} schedules")
			if schedule_delete_r:
				return schedule_delete_r
			return None

	def get_backup_data(self, resource_id: str, backup_id: str, timeout: int = 300) -> bytes | None:
		"""
		Retrieve backup data from BIMcloud by streaming the response.

		Args:
			resource_id (str): The resource ID.
			backup_id (str): The backup ID.
			timeout (int, optional): The request timeout in seconds.

		Returns:
			bytes: The downloaded backup data, or None if timed out.
		"""
		response = self.client.download_backup(resource_id, backup_id, timeout=timeout, stream=True)
		total_length = int(response.headers.get('content-length', 0))
		content = None
		downloaded = 0
		chunks = []
		start_time = time.time()
		last = start_time
		
		if response.ok:
			for chunk in response.iter_content(chunk_size=1024*64):
				if chunk:
					chunks.append(chunk)
					downloaded += len(chunk)
					now = time.time()
					runtime = now - start_time
					if runtime > timeout:
						self.log.error("Error (timeout?) during download", exc_info=True)
						self.report['errors'] += 1
						return None
					self.log.info(f"> receiving {round(downloaded/total_length*100)}%, runtime: {round(runtime)}/{round(timeout)} sec<rf>")
					last = now

			content = b''.join(chunks)
			self.log.info(f"> received {round(downloaded/total_length*100)}%, runtime: {round(runtime)}/{round(timeout)} sec<rf>")
			print ('', flush=True)

		else:
			self.log.error("Error during backup download", exc_info=True)
			self.report['errors'] += 1
			return None

		return content

	def transfer_backup(self, resource: dict, backup_id: str):
		"""
		Retrieve backup data from BIMcloud and upload it to Google Drive.

		Args:
			resource_name (str): The resource name.
			resource_id (str): The resource ID.
			resource_size (int): The resource file size.
			backup_id (str): The backup ID.

		Returns:
			None
		"""
		self.log.info(f"Get contents and save to the cloud...")
		timeout = self.get_timeout_from_filesize(resource['$size'], e=1.25) # adjusting for google
		data = self.get_backup_data(resource['id'], backup_id, timeout)
		if not data:
			logger.error(f"Failed to retreive backup data! Skipped.")
			self.report['errors'] += 1
			return None
		files = self.storage.get_folder_resources('1XKPjCnJJUunDn67wMgcQUoYargTmrOJ0')
		name = resource['name']+'.bim'+resource['type']+'25'
		match_file = next((f for f in files if f['name'] == name), None)
		match_file_id = match_file['id'] if match_file else None
		request = self.storage.prepare_upload(
			data,
			file_name = name,
			file_id = match_file_id,
			resource_id = resource['id']
		)
		upload = self.run_with_timeout(self.storage.upload_chunks, timeout, 0.05, request)
		if upload:
			self.log.info(f"Successfully uploaded to the cloud. ({upload['id']})")

		del data # just for case, considering large files


if __name__ == "__main__":

	start_time = time.time()
	errors = 0

	cmd = argparse.ArgumentParser()
	# cloud
	cmd.add_argument('-m', '--manager', required=True, help='URL of the BIMcloud Manager')
	cmd.add_argument('-c', '--client', required=True, help='Client Identification')
	cmd.add_argument('-u', '--user', required=True, help='User Login')
	cmd.add_argument('-p', '--password', required=True, help='User Password')
	cmd.add_argument('-f', '--filepath', required=False, help='Path to the log file')
	cmd.add_argument('-r', '--resource', required=False, help='Resource Id')
	# drive
	cmd.add_argument('-k', '--cred_path', required=True, help='Path to Gogole credentials')
	# notion
	cmd.add_argument('-n', '--notion', required=False, choices=['y', 'n'], default='y', help='Enable Notion reporting')
	arg = cmd.parse_args()

	setup(arg)

	cloud = BIMcloudAPI(**vars(arg))
	drive = GoogleDriveAPI(arg.cred_path, arg.client)
	notion = NotionAPI(arg.cred_path)
	manager = None

	try:
		if cloud and drive:
			manager = BackupManager(cloud, drive)
			manager.backup(arg.resource)
			status = "Done" if manager.report.get('errors', 0) == 0 else "Error"
		else:
			errors += 1
			status = "Fail"

	except Exception as e:
		logging.getLogger('BackupManager').error(f"Unexpected error: {e}", exc_info=True)
		errors += 1
		status = 'Fail'

	finally:
		stop_time = time.time()
		report_payload = {
			'items': manager.report.get('backups', 0) if manager else 0,
			'time': round(stop_time - start_time),
			'errors': manager.report.get('errors', errors) if manager else errors,
			'status': status,
		}
		if notion and arg.notion != 'n':
			notion.send_report(data=report_payload)
		logging.getLogger('BackupManager').info(f"Finished in {round(stop_time-start_time)} sec")