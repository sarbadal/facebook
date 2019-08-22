from processing.run import RunProcess

r = RunProcess()
r.get_adobject_data(ad_object='ad')  # campaign, adset, ad, creative
r.get_insights(saveto='data', data_limit=100)
