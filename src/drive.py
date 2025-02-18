import logging

from google.oauth2 import service_account
from googleapiclient.discovery import build

class GoogleDriveAPI():

    def __init__(self, cred_path, account):
        self.log = logging.getLogger("BackupManager")
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

        credentials = service_account.Credentials.from_service_account_file(
            cred_path,
            scopes=self.scopes
        ).with_subject(account)

        self.service = build('drive', 'v3', credentials=credentials)
        print (self.service)

    def get_files(self):
        results = self.service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(f"{item['name']} ({item['id']})")