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


class BackupManager():

	def __init__(self, client):
		self.client = client


	def backup(self) -> None:
		"""	Starts resource backup procedure.
		"""
		criterion = {
			'$or': [
				{'$eq': {'id': '9469F25B-D6DD-4CC3-8026-B85AC8338A16'}},
				{'$eq': {'id': '1FFABC82-3AEE-4509-B04D-E1789CEFFA62'}}
			]
		}
		resources = self.client.get_resources(criterion)
		for r in resources:
			print (f"Project: {r['id']}")
			has_outdated_backup = True
			backups = self.client.get_resource_backups([r['id']], params={'sort-by': '$time', 'sort-direction': 'desc'}) or None
			# check backups
			if backups:
				if backups[0].get('$time') >= r['$modifiedDate']:
					has_outdated_backup = False
				else:
					for b in backups:
						if b['$time'] <= r['$modifiedDate']:
							delete = self.delete_project_backup(r['id'], b['id'])
							print (f"Delete: {b['id']} {delete}")
			# create new if necessary
			if has_outdated_backup and r['type'] == 'project':
				ts = time.time()
				creation_job = self.create_project_backup(r['id'])
				is_valid = self.validate_project_backup(creation_job, ts)
				if is_valid:
					print ('OK')
			# don't hurry up
			time.sleep(1)

	def create_project_backup(self, resource_id: str):
		"""	Creates a new backup for project resource.
		Args:
			resource_id (str): Resource id
		Returns:
			bool: True if process succeded
		"""
		response, job = None, None
		response = self.client.create_resource_backup(resource_id, 'bimproject', 'Scripted Backup')
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
				time.sleep(2)

		if job['status'] == 'completed':
			print ('\n')
			return job
		return None

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

	def validate_project_backup(self, job, ts):
		"""	Validates created backup by checking it's existing & props.
		Args:
			job (dict): Job dictionary object, after creation is launched
			ts (int): Timestamp of backup creation
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
						{'$gte': {'$time': ts}}
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