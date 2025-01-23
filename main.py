import argparse
import json
import requests

from datetime import date, datetime, timedelta

class BIMcloud():

	def __init__(self, manager: str, client: str, user: str, password: str):
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

	def get_blob_content(self, session_id, blob_id):
		url = self.manager[:-1] + '1' + '/blob-store-service/1.0/get-blob-content'
		response = requests.get(url, params={'session-id': session_id, 'blob-id': blob_id}, stream=True)
		return response

	def get_model_servers(self):
		url = self.manager + '/management/client/get-model-servers'
		response = requests.get(url, headers=self.auth_header)
		return response.json()

	def get_resources(self, criterion={}):
		url = self.manager + '/management/client/get-resources-by-criterion'
		response = requests.post(url, headers=self.auth_header, params={}, json={**criterion})
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

	def start(self):
		log = bm.get_backup_log()
		if log:
			blob = self.client.get_blob_content(self.client.session['id'], log['id'])
			modified = self.get_modified(log['$modifiedDate'])
		if blob and modified:
			name = f"job_backup_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
			data = blob.json()
			for m in modified:
				print (f"{m['id']} {m['name']}")
				data[name] = len(modified)

				write = self.write_data(data, '/_LOG/job_backup.json')

	def get_backup_log(self):
		blobs = self.client.get_resources(
			criterion = {
				'$and': [
					{'$eq': {'type': 'blob'}},
					{'$eq': {'$parentName': '_LOG'}}
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
		binary = json.dumps(data, indent = 4).encode('utf-8')

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
	cmd.add_argument('-c', '--client', required=False, help='Client Identification')
	cmd.add_argument('-u', '--user', required=False, help='User Login')
	cmd.add_argument('-p', '--password', required=False, help='User Password')
	arg = cmd.parse_args()

	try:
		bc = BIMcloud(**vars(arg))
		bm = BackupManager(bc)

		bm.start()

		# data = {
		#     "job_2025-01-23_01:47:17": [
		#         "3DC3CD2A-3BE1-47A8-930E-093C60C1E871",
		#         "F6620323-3A84-4CEB-A24C-DC5BECD15C26",
		#         "767FBC81-6785-4364-8BE7-8BBA73CA27E4",
		#         "6C1C2047-14E4-4EFB-97A8-EB225A780F31",
		#         "BF17CC81-A0E8-4D64-9456-7E37BD2896F2",
		#         "A8A1DA3E-84E2-4E60-B3E3-412A56AE65BD",
		#         "39FBBB2A-A5D2-4446-83BB-619E950320EA"
		#     ],
		#     "job_2025-02-25_01:47:17": [
		#         "3DC3CD2A-3BE1-47A8-930E-093C60C1E871",
		#         "F6620323-3A84-4CEB-A24C-DC5BECD15C26",
		#         "767FBC81-6785-4364-8BE7-8BBA73CA27E4"
		#     ]
		# }
		# data = [
		#     {"job_2025-01-23_01:47:17": [
		#         "3DC3CD2A-3BE1-47A8-930E-093C60C1E871",
		#         "F6620323-3A84-4CEB-A24C-DC5BECD15C26",
		#         "767FBC81-6785-4364-8BE7-8BBA73CA27E4",
		#         "6C1C2047-14E4-4EFB-97A8-EB225A780F31",
		#         "BF17CC81-A0E8-4D64-9456-7E37BD2896F2",
		#         "A8A1DA3E-84E2-4E60-B3E3-412A56AE65BD",
		#         "39FBBB2A-A5D2-4446-83BB-619E950320EA"
		#     ]},
		#     {"job_2025-02-25_01:47:17": [
		#         "3DC3CD2A-3BE1-47A8-930E-093C60C1E871",
		#         "F6620323-3A84-4CEB-A24C-DC5BECD15C26",
		#         "767FBC81-6785-4364-8BE7-8BBA73CA27E4"
		#     ]}
		# ]

		# bm.write_data(data, '/_LOG/job_backup.json')
		# job = bm.get_last_job()
		# job_date = datetime.strptime(job['job_updated'], '%Y-%m-%d %H:%M:%S')
		# job_time = int(job_date.timestamp())

		# res = bm.get_modified(from_time=job_time)
		# print(json.dumps(res, indent = 4))
		# for r in res:
		# 	print (r['name'])

	except:
		raise