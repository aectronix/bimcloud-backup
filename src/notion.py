import json
import logging
import requests

from urllib3.util.retry import Retry

class NotionAPI():

	def __init__(self, cred_path):
		self.log = logging.getLogger("BackupManager")
		self.credentials = json.load(open(cred_path)).get('notion') or None
		self._auth = {}

		self._r = self._setup_requests()
		self.authorize()

	def _setup_requests(self):
		"""
		Initialize a requests.Session with a retry adapter.

		Returns:
			requests.Session: Configured session.
		"""
		adapter = requests.adapters.HTTPAdapter(
			max_retries=Retry(
				total=3,
				backoff_factor=1,
				status_forcelist=[429, 500, 502, 503, 504],
				allowed_methods=['GET', 'POST', 'DELETE',]
			)
		)
		session = requests.Session()
		session.mount("https://", adapter)
		session.mount("http://", adapter)
		return session

	def _send_request(self, method: str, url: str, **kwargs):
		"""
		Refresh the token/session if necessary and send an HTTP request.

		Args:
			method (str): HTTP method (e.g. 'GET', 'POST', 'DELETE').
			url (str): URL to request.
			**kwargs: Additional keyword arguments (e.g. timeout, stream, params, etc.).
			Use the 'stream' key to indicate if the raw response should be returned.

		Returns:
			The parsed JSON response or raw response based on the 'stream' flag.

		Raises:
			RuntimeError: If the HTTP response is not OK.
		"""
		# self.refresh_on_expiration()
		headers_extra = kwargs.pop('headers', {})
		headers = {**self._auth['headers'], **headers_extra}
		response = self._r.request(method.upper(), url, headers=headers, **kwargs)
		return self._take_response(response)

	def _take_response(self, response: requests.Response):
		"""
		Process the HTTP response.

		Args:
			response (requests.Response): The HTTP response.
			raw_stream (bool): If True, return the raw response; otherwise, parse JSON.

		Returns:
			Parsed JSON data if response has content and raw_stream is False;
			raw response if raw_stream is True;
			or None if there is no content.

		Raises:
			RuntimeError: If the HTTP response status is not OK.
		"""
		has_content = response.content is not None and len(response.content)
		if response.ok:
			if has_content:
				return response.json()
			else:
				return None
		raise RuntimeError(f"Response Error {response}")

	def authorize(self):
		try:
			self._auth['token'] = self.credentials.get('secret')
			self._auth['headers'] = {
			    'Notion-Version': '2022-06-28',
			    'Content-Type': 'application/json',
			    'Authorization': 'Bearer ' + self._auth['token'],
			}
			self.log.info(f"Notion initialized")
		except Exception as e:
			raise RuntimeError("Notion authorization failed") from e

	def send_report(self, data):
		self.log.info(f"Sending report...")
		query = {
			'parent': { 'database_id': None},
			'properties': {
				'Name': {
					'id': 'title',
					'type': 'title',
					'title': [
						{
							'type': 'text',
							'text': {
								'content': 'Backup'
							}
						}
					]
				},
				'Version': {
					'type': 'rich_text',
					'rich_text': [
						{
							'type': 'text',
							'text': {
								'content': 'v25'
							},
							'annotations': {'bold': True}
						}
					]
				},
				'Status': {
					'type': 'status',
					'status': {
						# 'id': 'a96490cc-6128-483e-b171-1ad54127504b',
						'name': data.get('status', None),
						# 'color': 'green'
					}
				},
				'Errors': {
					'type': 'number',
					'number': data.get('errors', None)
				},
				'Job Time': {
					'type': 'number',
					'number': data.get('time', None)
				},
				'Items': {
					'type': 'number',
					'number': data.get('items', None)
				}
			}
		}
		request = self.add_page(query, '1acb695243cd8003af22cbacf66abc50')

	def get_database(self, db_id):
		url = 'https://api.notion.com/v1/databases/'+db_id+'/query'
		response = self._send_request('post', url)
		return response

	def add_page(self, query, db_id=None):
		if db_id:
			query['parent']['database_id'] = db_id
		url = 'https://api.notion.com/v1/pages'
		response = self._send_request('post', url, json=query)
		return response