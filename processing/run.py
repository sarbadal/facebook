# encoding=utf-8

import os

from utils.getData import GetInsightsAdObjects
from processing.collectparams import CollectCredentialsParams
from settings.settings import BASE_DIR


class RunProcess:
    """ Run the process and drop results in the designated S3 location. """

    def __init__(self):
        self.__fields = self._set_fields()
        self.__params = self._set_params()
        self.__insight_adobj = self.initialise_get_insight_adobj()

    def display_params(self):
        """Print params in json format"""
        import json
        print(json.dumps(self.__params, indent=4))

    def display_fields(self):
        """Print fields"""
        import json
        print(json.dumps(self.__fields, indent=4))

    @property
    def params(self):
        """Return params"""
        return self.__params

    @property
    def fields(self):
        """Return fields"""
        return self.__fields

    @params.setter
    def params(self, params):
        self.__params = params

    @fields.setter
    def fields(self, fields):
        self.__fields = self._set_fields(fields)

    def _set_fields(self, fields=None):
        """Download fields from params/fieldlist.json file"""
        import json

        if fields is None:
            filename = os.path.join(BASE_DIR, 'params', 'fieldlist.json')
            with open(filename, 'r') as f:
                return json.load(f)['fields']

        elif isinstance(fields, list):
            return fields

        else:
            with open(fields, 'r') as f:
                return json.load(f)['fields']

    def set_fields(self, fields):
        """Set fields method"""
        self.__fields = self._set_fields(fields=fields)

    def _set_params(self):
        """ Download params using class 'CollectCredentialsParams'. """
        cp = CollectCredentialsParams()
        return [p for p in cp.process_params()]

    def initialise_get_insight_adobj(self, account_ids=None, business_ids=None, credentils=None):
        """ Initialise the GetInsightsAdObjects class """
        insight_ad_obj = GetInsightsAdObjects(
            account_ids=account_ids,
            business_ids=business_ids,
            credentils=credentils
        )
        insight_ad_obj.initialize()

        return insight_ad_obj

    def get_insights(self, saveto='data', data_limit=250):
        """Download FB Insight Data"""

        if not os.path.exists(os.path.join(BASE_DIR, saveto)):
            os.makedirs(os.path.join(BASE_DIR, saveto))

        iaobj = self.__insight_adobj
        field = self.fields

        for param, f in self.params:
            df = iaobj.get_insights(
                fields=field, 
                params=param, 
                data_limit=data_limit
            )
            df.to_csv(
                os.path.join(BASE_DIR, saveto, f),
                sep=',',
                header=True,
                encoding='utf-8',
                index=False
            )

    def get_adobject_data(self, saveto='data', ad_object=None, outfile=None):
        """Download Ad Obj Info"""

        if not os.path.exists(os.path.join(BASE_DIR, saveto)):
            os.makedirs(os.path.join(BASE_DIR, saveto))

        iaobj = self.__insight_adobj

        if outfile is None:
            outfile = f'fb_{ad_object}.csv'

        df = iaobj.get_adobject_info(ad_object=ad_object)
        df.to_csv(
            os.path.join(BASE_DIR, saveto, outfile),
            sep=',',
            header=True,
            encoding='utf-8',
            index=False
        )


if __name__ == '__main__':
    r = RunProcess()
    r.get_adobject_data(ad_object='campaign')
    r.get_insights(saveto='data', data_limit=100)
