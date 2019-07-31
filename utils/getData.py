import os
import datetime

import warnings
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.user import User
from facebookads.adobjects.business import Business
from facebookads.adobjects.adaccount import AdAccount

from settings.settings import BASE_DIR

warnings.filterwarnings("ignore")


class GetInsightsAdObjects:
    """
    Base class to Extract adobjects info and insights for option
    Business and Ad Account list.
    """

    api_version = 'v3.3'  # Facebook API version

    def __init__(self, account_ids=None, business_ids=None, credentils=None):
        self.account_ids = account_ids
        self.business_ids = business_ids
        self.__app_id, self.__app_secret, self.__access_token = self.load_credentials(credentils, file=False)


    def initialize(self, app_id=None, app_secret=None, access_token=None):
        """Initialize the FB API"""
        if self.app_id and self.app_secret and self.access_token:

            FacebookAdsApi.init(
                self.app_id,
                self.app_secret,
                self.access_token,
                api_version=self.__class__.api_version
            )

            return True

        elif app_id and app_secret and access_token:

            FacebookAdsApi.init(
                app_id,
                app_secret,
                access_token,
                api_version=self.__class__.api_version
            )

            return True

        else:
            print('app_id, app_secret, and access_token must be set.')
            return False


    @property
    def app_id(self):
        """Return App ID"""
        return self.__app_id

    @app_id.setter
    def app_id(self, app_id):
        self.__app_id = app_id

    @property
    def app_secret(self):
        """Return App Sccrect"""
        return self.__app_secret

    @app_secret.setter
    def app_secret(self, app_secret):
        self.__app_secret = app_secret

    @property
    def access_token(self):
        """Return App Sccrect"""
        return self.__access_token

    @access_token.setter
    def access_token(self, access_token):
        self.__access_token = access_token


    @staticmethod
    def load_credentials(credentils_json=None, file=False):
        """
        Loding credentials from json file
        Default locatiom: credentials\credentials.json file
        """
        import json

        if credentils_json is None:
            filename = os.path.join(BASE_DIR, 'credentials', 'credentials.json')

            with open(filename, 'r') as f:
                c = json.load(f)['APP']
                return c['APP_ID'], c['APP_SECRET'], c['ACCESS_TOKEN']

        elif file:
            with open(file, 'r') as f:
                c = json.load(f)['APP']
                return c['APP_ID'], c['APP_SECRET'], c['ACCESS_TOKEN']

        else:
            try:
                c = credentils_json
                return c['APP_ID'], c['APP_SECRET'], c['ACCESS_TOKEN']

            except:
                try:
                    c = credentils_json['APP']
                    return c['APP_ID'], c['APP_SECRET'], c['ACCESS_TOKEN']
                except:
                    print("Couldn't load credentials")
                    return None, None, None

    @staticmethod
    def load_ad_obj_fields(filename=None, ad_object=None):
        """Load field name for the corresponding ad object."""
        import json

        if filename is None:
            filename = os.path.join(BASE_DIR, 'credentials', 'adobjectfields.json')

        if ad_object is not None:

            with open(filename, 'r') as f:
                return json.load(f)[ad_object.lower()]

        else:
            return None


    @staticmethod
    def _clean_account_id(txt):
        """
        Remove special characters and any chars from number text and
        convert that text into digit string.
        Return clean str version of digits.
        """
        chars_to_keep = [str(n) for n in range(0, 10, 1)]
        txt_str = str(txt)

        cln_txt = ''.join([c for c in txt_str if c in chars_to_keep])
        cln_txt = cln_txt if len(cln_txt) > 0 else '0'

        return str(cln_txt)

    @staticmethod
    def _wait_for_async_job(job, data_limit=25, TIMEOUT=3600):
        """Wait until job is complete."""
        import time
        from facebookads.adobjects.adreportrun import AdReportRun

        for _ in range(TIMEOUT):
            time.sleep(1)
            job = job.remote_read()
            status = job[AdReportRun.Field.async_status]
            if status == 'Job Completed':
                return job.get_result(params={"limit": data_limit})


    def _define_account(self):
        """
        It takes account and business list or None (befault)
        and extract only relivant AdAccounts.
        Return list of AdAccount
        """
        if self.account_ids and not self.business_ids:
            return {
                'BusinessID': {
                    'Unknown': [AdAccount('act_' + self._clean_account_id(_id)) for _id in self.account_ids]
                }
            }

        elif not self.account_ids and not self.business_ids:
            me = User(fbid='me')
            return {
                'BusinessID': {
                    'Unknown': me.get_ad_accounts()
                }
            }

        elif not self.account_ids and self.business_ids:
            business = [Business(self._clean_account_id(_id)) for _id in self.business_ids]
            result = {
                'BusinessID': {}
            }

            for _business in business:
                result['BusinessID'][_business['id']] = []
                for _account in _business.get_owned_ad_accounts():
                    result['BusinessID'][_business['id']].append(_account)
                count_down(60)
            return result

        elif self.account_ids and self.business_ids:
            accounts_in_userlist = [AdAccount('act_' + self._clean_account_id(_id)) for _id in self.account_ids]
            business = [Business(self._clean_account_id(_id)) for _id in self.business_ids]
            result = {
                'BusinessID': {}
            }

            for _business in business:
                result['BusinessID'][_business['id']] = []
                for _account in _business.get_owned_ad_accounts():
                    result['BusinessID'][_business['id']].append(_account)

                result['BusinessID'][_business['id']] = [
                    _account for _account in accounts_in_userlist
                    if _account in result['BusinessID'][_business['id']]
                ]
                count_down(60)
            return result

        else:
            return None


    @property
    def ad_acccounts(self):
        """
        It takes account and business list or None (befault)
        and extract only relivant AdAccounts.

        Return list of AdAccount ID
        """
        return self._define_account


    def get_adobject_info(self, ad_object='creative', verbose=True):
        """
        Get the Creative attributes
        ad_object: creative, ad, adset or campaign
        """
        import time
        import pandas as pd

        if ad_object.lower() == 'creative':
            ad_obj = 'get_ad_creatives'

        elif ad_object.lower() == 'ad':
            ad_obj = 'get_ads'

        elif ad_object.lower() == 'adset':
            ad_obj = 'get_ad_sets'

        elif ad_object.lower() == 'campaign':
            ad_obj = 'get_campaigns'

        else:
            print(f'{ad_object} is not valid. Valid objects are "creative", "ad", "adset", and "campaign"')
            return None

        text_to_execute = f"account.{ad_obj}(fields=fields['name'])"
        fields = self.load_ad_obj_fields(ad_object=ad_object)

        ad_obj_df = pd.DataFrame()
        acccount_list = self.ad_acccounts()['BusinessID']

        for business, accounts in acccount_list.items():
            for account in accounts:
                time.sleep(30)

                ad_id, df = account['id'], pd.DataFrame()
                if verbose:
                    print(f'Fetching data for account {ad_id}...', end='')

                for i, creative in enumerate(eval(text_to_execute)):
                    fields['pd_field_list'] = [[] for _ in fields['name']]
                    time.sleep(1)

                    for name, pd_field_list in zip(fields['name'], fields['pd_field_list']):
                        pd_field_list.append(creative.get(name, float('nan')))

                    tmp_df = pd.DataFrame(dict(zip(fields['name'], fields['pd_field_list'])))
                    if business != 'Unknown':
                        tmp_df['Business_ID'] = business

                    df = pd.concat([df, tmp_df], axis=0, ignore_index=True, sort=False)

                if verbose: print(' Done')
                ad_obj_df = pd.concat([ad_obj_df, df], axis=0, ignore_index=True, sort=False)

        return ad_obj_df


    def get_insights(self, fields=None, params=None, data_limit=500, verbose=True):
        """Return dataFrame of Facebook AdInsight data"""
        import pandas as pd
        import time

        insight_df = pd.DataFrame()
        acccount_list = self.ad_acccounts()['BusinessID']

        for business, accounts in acccount_list.items():
            for account in accounts:
                ad_id, df = account['id'], pd.DataFrame()

                time.sleep(10)

                if verbose:
                    print(f'Fetching data for account {ad_id}...', end='')

                job = account.get_insights_async(fields=fields, params=params)
                result_cursor = self._wait_for_async_job(job, data_limit=data_limit)

                if result_cursor is not None:

                    action_att_list = params.get('action_attribution_windows', []).copy()
                    action_att_list.append('value')

                    for i, stats in enumerate(result_cursor):

                        # Slowdown the hit...
                        if i % data_limit == 0 and i > 0:
                            time.sleep(300)

                        values, names = [], []
                        for stat in stats:
                            if type(stats[stat]) == type('abc'):
                                values.append([stats[stat]])
                                names.append(stat.lower())

                            if type(stats[stat]) == type([]):

                                for dict_values in stats[stat]:  # dict_values is a Python dict
                                    col = stat

                                    for key, value in dict_values.items():
                                        if key not in action_att_list:
                                            col += '|' + key + '=' + str(value)

                                    for att_window in action_att_list:
                                        if att_window == 'value':
                                            field = col
                                        else:
                                            field = col + '|' + att_window

                                        names.append(field)
                                        values.append([dict_values.get(att_window, '0')])

                        tmp_df = pd.DataFrame(dict(zip(names, values)))

                        if business != 'Unknown':
                            tmp_df['Business_ID'] = business

                        df = pd.concat([df, tmp_df], axis=0, ignore_index=True, sort=False)

                    if verbose: print(' Done')

                    insight_df = pd.concat([insight_df, df], axis=0, ignore_index=True, sort=False)

                else:
                    if verbose:
                        print(f'Time out... Data for Ad Account {ad_id} will not be pulled.')

        return insight_df


if __name__ == '__main__':
    import pandas as pd
    from facebookads.adobjects.adsinsights import AdsInsights

    # Set Print option
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 50)

    params = {
        'time_range': {
            'since': '2019-05-01',
            'until': '2019-05-01'
        },
        'breakdowns': ['device_platform', 'publisher_platform', 'platform_position'],
        'action_breakdowns': ['action_type'],
        'level': "campaign",
        'time_increment': 'all_days'
    }

    field = [
        'account_name',
        'account_id',
        'campaign_name',
        'campaign_id',
        'account_currency',
        'buying_type',
        'impressions',
        'clicks',
        'spend',
        'unique_clicks',
        'actions',
        'action_values',
        'ad_name',
        'ad_id',
        'adset_name',
        'adset_id',
        'objective',
        'reach',
        'frequency',
        'video_10_sec_watched_actions',
        'video_p100_watched_actions',
        'video_avg_time_watched_actions'
    ]

    fb = GetInsightsAdObjects()
    fb.initialize()

    # df = fb.get_insights(fields=field, params=params)
    df = fb.get_adobject_info(ad_object='campaign')
    print(df.head(n=30))
