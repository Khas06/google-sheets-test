from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import time

from cbr_services import get_daily_rate
from database import create_data, read_data, update_data, delete_data

SERVICE_ACCOUNT_FILE = 'creds.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

SAMPLE_SPREADSHEET_ID = '1c3uqf5tfrilZ1VewJ7UT_lEt8HRMa6AaedgLeHWRvPU'


def main():
    while True:
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="Лист1!A1:D100").execute()

            data = result.get('values', [])[1:]
        except HttpError as err:
            print(err)
        curs_dol = get_daily_rate()
        formatted_data = tuple([tuple(i) + (format(int(i[2]) * curs_dol, '.2f'),) for i in data])

        if not read_data():
            create_data(formatted_data)

        data_from_db = read_data()
        data_changes = set(formatted_data).difference(set(data_from_db))
        data_for_del = set(data_from_db).difference(set(formatted_data))

        if data_changes:
            create_data(data_changes)
        elif data_for_del:
            delete_data(data_for_del)


if __name__ == '__main__':
    main()
    time.sleep(20)
