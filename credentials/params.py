# encoding=utf-8

import os
import json
from utils.s3FuncTools import FileTransferS3
from settings.settings import BASE_DIR


filename = os.path.join(BASE_DIR, 'params', 'params.json')


def get_params_from_s3(filename=filename, download=False):
    """
    Download json file which contains
    list of report parameters from S3
    """
    if download:
        ft = FileTransferS3()

        try:
            ft.downloadfile_from_s3(
                out_file_path=os.path.join(BASE_DIR, 'params'),
                files=['fb_report_params/params.json']
            )
        except Exception:
            print('Could not update the report scope from s3. '
                  'Default report-params will be used.')

    with open(filename, 'r') as f:
        params = json.load(f)

    return params


if __name__ == '__main__':
    params = get_params_from_s3(download=True)
    print(params)
