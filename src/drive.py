import json
import io
import logging
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.http import MediaFileUpload

class GoogleDriveAPI():

    def __init__(self, cred_path, account):
        """
        Initialize the GoogleDriveAPI instance.

        Args:
            cred_path (str): Path to the Google credentials JSON file.
            account (str): The email address or account identifier to impersonate.
        """
        self.log = logging.getLogger("BackupManager")
        self.scopes = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive.metadata',
        ]
        self.service = None

        self.authorize(cred_path, account)

    def authorize(self, cred_path, account):
        """
        Authorize with Google Drive using a service account.

        Args:
            cred_path (str): Path to the credentials JSON.
            account (str): The email or account to impersonate.

        Raises:
            RuntimeError: If authorization fails.
        """
        try:
            service_account_info = json.load(open(cred_path))
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info['google_drive'],
                scopes = self.scopes
            ).with_subject(account)
            service = build('drive', 'v3', credentials=credentials)
            if service:
                self.service = service
                self.log.info(f"Cloud storage initialized: {service._baseUrl} ({account.split('@')[0]})")
        except Exception as e:
            raise RuntimeError("Google Drive authorization failed") from e

    def get_folder_resources(self, folder_id):
        """ Retrieve the list of files in a given folder. """
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
        """
        Prepare an upload request for a file to Google Drive.

        Args:
            data (bytes): The file data.
            file_name (str): The name of the file.
            file_id (str, optional): The file ID to update (if any).
            **kwargs: Additional keyword arguments, e.g. 'resource_id' for file description.

        Returns:
            A Drive API request object ready for upload.
        """
        file_stream = io.BytesIO(data)
        file_stream.seek(0)
        file_metadata = {
            'name': file_name,
            'description': kwargs.get('resource_id', None)
        }
        media = MediaIoBaseUpload(
            file_stream,
            mimetype = 'application/octet-stream',
            chunksize = 1024*1024*5, # 5MB is max
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
        """
        Upload file content in chunks using a resumable upload request.

        Args:
            request: A resumable upload request object.
            **kwargs: Additional keyword arguments (e.g. runtime, timeout).

        Returns:
            The final response of the upload (e.g. file metadata) upon completion.
        """
        response = None
        status, response = request.next_chunk()
        if status:
            self.log.info(f"> uploading: {int(status.progress() * 100)}%, runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec<rf>")
        if response:
            self.log.info(f"> uploaded: 100%, runtime: {round(kwargs.get('runtime'))}/{round(kwargs.get('timeout'))} sec<rf>")
            print ('', flush=True)
        return response