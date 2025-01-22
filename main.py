import argparse
import json
import requests

class BIMcloud():

	def __init__(self, manager: str, client: str, user: str, password: str):
		self.manager = manager
		self.client = client
		self.user = user
		self.password = password
		self.auth = None
		self.auth_header = None
		self.session = None

		self.authorize()
		self.grant()

	def authorize(self):
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

	def grant(self):
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

	def get_model_servers(self):
		url = self.manager + '/management/client/get-model-servers'
		response = requests.get(url, headers=self.auth_header)
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

		bm.write_data({"qwerty": 123}, '/_BCP/backup.json')

	except:
		raise
