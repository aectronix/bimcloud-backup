import io
import logging
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.http import MediaFileUpload

class GoogleDriveAPI():

    def __init__(self, cred_path, account):
        self.log = logging.getLogger("BackupManager")
        self.scopes = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive.metadata',
        ]
        self.service = None

        self.authorize(cred_path, account)

    def authorize(self, cred_path, account):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                cred_path,
                scopes = self.scopes
            ).with_subject(account)
            service = build('drive', 'v3', credentials=credentials)
            if service:
                self.service = service
                self.log.info(f"Cloud storage initialized: {service._baseUrl} ({account.split('@')[0]})")
        except Exception as e:
            self.log.error(f"Auth Error: {e}", exc_info=True)
            sys.exit(1)

    def execute_request(self, queue, request):
        try:
            result = request.execute()
            queue.put(result)
        except Exception as e:
            queue.put(e)

    def get_folder_resources(self, folder_id):
        try:
            result = self.service.files().list(
                q = f"'{folder_id}' in parents",
                pageSize = 1000,
                fields = "nextPageToken, files(id, name, modifiedTime)"
            ).execute()
            return result.get('files', [])
        except Exception as e:
            self.log.error(f"Root folder error: {e}", exc_info=True)
            sys.exit(1)

    def prepare_upload(self, data, file_name, file_id=None, **kwargs):
        file_stream = io.BytesIO(data)
        file_stream.seek(0)
        file_metadata = {
            'name': file_name,
            'description': kwargs.get('resource_id', None)
        }
        media = MediaIoBaseUpload(
            file_stream,
            mimetype = 'application/octet-stream',
            chunksize = 1024*1024*5, # max
            resumable = True
        )
        params = {
            'body': file_metadata,
            'media_body': media,
            'fields': 'id, parents, description'
        }
        if file_id:
            params['fileId'] = file_id
            request = self.service.files().update(**params)
        else:
            params['body']['parents'] = ['1XKPjCnJJUunDn67wMgcQUoYargTmrOJ0']
            request = self.service.files().create(**params)
        return request

    def upload_chunks(self, request, **kwargs):
        response = None
        status, response = request.next_chunk()
        if status:
            self.log.info(f"> uploading: {int(status.progress() * 100)}%, runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec<rf>")
        if response:
            self.log.info(f"> uploaded: 100%, runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec<rf>")
            print ('', flush=True)
        return response