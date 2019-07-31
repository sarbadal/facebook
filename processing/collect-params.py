import os

from utils.s3FuncTools import FileTransferS3
from settings.settings import BASE_DIR

import warnings
warnings.filterwarnings("ignore")


class CollectCredentialsParams:
    """Download Params JSON file and process."""

    def __init__(self):
        self.__ft = FileTransferS3()


    def load_all(self):
        """Load all params/ fields/ s3 credentials"""
        self.load_s3_credentils()
        self.load_fb_params()
        self.load_fb_fields()


    def load_s3_credentils(self):
        """Load Credentils from credentils/credentisl.json file"""
        import json

        try:
            self.__ft.downloadfile_from_s3(
                out_file_path=os.path.join(BASE_DIR, 'credentials'),
                files=['credentials/credentials.json']
            )
        except:
            print('Could not update the S3 credentils from s3. Default credentils will be used.')


    def load_fb_params(self):
        """Load FB params from S3"""
        import json

        try:
            self.__ft.downloadfile_from_s3(
                out_file_path=os.path.join(BASE_DIR, 'params'),
                files=['credentials/params/params.json']
            )
        except:
            print('Could not update the params from s3. Default params will be used.')


    def load_fb_fields(self):
        """Load FB params from S3"""
        import json

        try:
            self.__ft.downloadfile_from_s3(
                out_file_path=os.path.join(BASE_DIR, 'params'),
                files=['credentials/params/fieldlist.json']
            )
        except:
            print('Could not update the fields from s3. Default fields will be used.')


    def process_params(self):
        """Process and Update params for the final use."""
        import datetime
        import json

        filename = os.path.join(BASE_DIR, 'params', 'params.json')
        TODAY = datetime.date.today()
        with open(filename, 'r') as f:
            params = json.load(f)

        collect_param, outfiles, v = [], [], 1
        for breakdown in params:
            param = params[breakdown]

            if param['time_range'].lower() == 'yesterday':
                v = 1
            elif param['time_range'].lower() == 'last_2_days':
                v = 2
            elif param['time_range'].lower() == 'last_5_days':
                v = 5
            elif param['time_range'].lower() == 'last_7_days':
                v = 7
            else:
                print(f'time_range must be one of these - yesterday, last_2_days, last_5_days or last_7_days')
                return None

            for i in range(v):
                param['time_range'] = {
                    'since': str(TODAY - datetime.timedelta(days=i + 1)),
                    'until': str(TODAY - datetime.timedelta(days=i + 1))
                }
                outfile = 'fb_breakdown_' + str(breakdown).lower().replace(' ', '')
                outfile += f'_{str(TODAY - datetime.timedelta(days=i + 1))}'
                outfile += f'-{str(TODAY - datetime.timedelta(days=i + 1))}.csv'
                collect_param.append(param)
                outfiles.append(outfile)

        return zip(collect_param, outfiles)




if __name__ == '__main__':
    params = CollectCredentialsParams()
    for i in params.process_params():
        print(i[0])
