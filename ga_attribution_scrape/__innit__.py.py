##import packages

import google.auth
from google.auth.transport import requests
from apiclient.discovery import build  # pip install --upgrade google-api-python-client
from oauth2client.service_account import ServiceAccountCredentials  # pip3 install --upgrade oauth2client
from google.oauth2 import service_account

import requests
import pandas as pd
import io
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, date as d, timedelta
import time


def get_ga_goals(credentials, account_id, property_id, view_id):

    # Build analytic authentication
    SCOPES = ['https://www.googleapis.com/auth/analytics', "https://www.googleapis.com/auth/analytics.edit",
              "https://www.googleapis.com/auth/analytics.readonly"]

    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials, scopes=SCOPES)

    analytics = build('analytics', 'v3', credentials=credentials)

    ###### Get  GA Goals
    goal_response = analytics.management().goals().list(
        accountId=account_id, webPropertyId=property_id, profileId=view_id)\
        .execute()
    goal_response = goal_response["items"]
    goals = pd.DataFrame(goal_response)
    goals = goals[goals.active == True]
    goals = goals.reset_index()

    return goals


class Scrape:

    def __init__(self, configuration):
        self.configuration = configuration

    def goals(self, ga_management_data):
        request_url = self.configuration['ga_attribution_scrape']['request']['url']
        request_headers = self.configuration['ga_attribution_scrape']['request']['headers']
        form_data = self.configuration['ga_attribution_scrape']['request']['form_data']
        backdate = self.configuration['ga_attribution_scrape']['backdate']['backdate']

        # From configuration backdate or not. If not then get yesterday.
        if backdate:
            start_date = self.configuration['ga_attribution_scrape']['backdate']['start_date']
            end_date = self.configuration['ga_attribution_scrape']['backdate']['end_date']
            date_range = []
            for date in pd.date_range(start=start_date, end=end_date):
                date_range.append(date.strftime('%Y%m%d'))
        else:
            yesterday = d.today() - timedelta(days=1)
            date_range = [str(yesterday.strftime('%Y%m%d'))]

            # Loop through each day for each goal for DDA
        print('Getting attribution response for each day for each relevant goal ID')
        ga_attribution_data = pd.DataFrame()
        for date in date_range:
            for row in ga_management_data.id:

                # Filter goals to row goal - we'll use this to manipulate response and final data output
                conversion = ga_management_data[ga_management_data.id == row]

                # Add dates into form data for response
                form_data['_u.date00'] = date
                form_data['_u.date01'] = date

                # Add conversion ID into response
                conversion_id = conversion.id.to_list()[0]
                form_data['_.bfType'] = conversion_id

                # Get raw response data for GA
                raw_response = requests.post(request_url, data=form_data, headers=request_headers).text
                #print(raw_response)
                # Check for http error in response
                while 'The service is temporarily unavailable. Please try again in a few minutes.' in raw_response:
                    print("http error: The service is temporarily unavailable. Please try again in a few minutes.")
                    print("Sleeping for a couple of mins then retry")
                    time.sleep(120)
                    raw_response = requests.post(request_url, data=form_data, headers=request_headers).text

                #print(raw_response)
                time.sleep(1.5)

                # Clean response
                # Cleaning the actual response by removing the unnecessary lines and adding in other variables
                response = pd.read_csv(io.StringIO(raw_response), quotechar='"', skipinitialspace=True,
                                       error_bad_lines=False,
                                       skiprows=5)[:-3]

                # Renaming the columns
                new_column_names = []
                for item in response.columns:
                    x = item.replace(" ", "_")
                    x = x.replace("-", "_")
                    if x[0].isdigit():
                        x = "_" + x
                    new_column_names.append(x.lower())
                response.columns = new_column_names
                response = response[response.columns.drop(list(response.filter(regex='%_change_')))]

                # Change the types to float and removing all special characters
                for item in response.columns:
                    if 'spend' in item or 'data_driven' in item:
                        response[item] = response[item].replace({'£|€|$|>|<|,|\\%': ''}, regex=True)
                        response[item] = response[item].astype(float)

                # Make data into pandas dataframe
                clean_data = pd.DataFrame(response, columns=response.columns)

                # Add in the conversion name and id of goals
                clean_data['conversion_name'] = conversion.name.to_list() * len(clean_data.index)
                clean_data['conversion_id'] = conversion.id.to_list() * len(clean_data.index)

                # Add date into data
                clean_data["date"] = datetime.strptime(date, '%Y%m%d')

                ga_attribution_data = pd.concat([ga_attribution_data, clean_data])
                print(ga_attribution_data)

        return ga_attribution_data

    def ecommerce(self):
        request_url = self.configuration['ga_attribution_scrape']['request']['url']
        request_headers = self.configuration['ga_attribution_scrape']['request']['headers']
        form_data = self.configuration['ga_attribution_scrape']['request']['form_data']

'''
def scrape(configuration, conversion_type=None, goal_management_data=None, conversion_ids=None, request_date=None, yaml_data=None, market=None):

    ecommerce = configuration['ga_attribution_scrape']['ga']['ecommerce']
    request_url = configuration['ga_attribution_scrape']['request']['url']
    request_headers = configuration['ga_attribution_scrape']['request']['headers']
    form_data = configuration['ga_attribution_scrape']['request']['form_data']

    if conversion_type == 'goals' and goal_management_data is not None:
        conversion_ids = goal_management_data.id
    elif conversion_type == 'ecommerce':
        conversion_ids = ['Ecommerce']
    elif conversion_ids is not None:
        conversion_ids = conversion_ids


    print(conversion_ids)


    ga_attribution_data = pd.DataFrame()
    for conversionId in conversion_ids:

        print(conversionId)

        form_data['_.bfType'] = conversionId

        raw_response = requests.post(request_url, data=form_data, headers=request_headers).text
        # Check for http error in response
        while 'The service is temporarily unavailable. Please try again in a few minutes.' in raw_response:
            print("http error: The service is temporarily unavailable. Please try again in a few minutes.")
            print("Sleeping for a couple of mins then retry")
            time.sleep(120)
            raw_response = requests.post(request_url, data=form_data, headers=request_headers).tex

        print(raw_response)
        time.sleep(1.5)

        # Cleaning the actual response by removing the unnecessary lines and adding in other variables
        response = pd.read_csv(io.StringIO(raw_response), quotechar='"', skipinitialspace=True, error_bad_lines=False,
                               skiprows=5)[:-3]

        #print(response)

        # Renaming the columns
        new_column_names = []
        for item in response.columns:
            x = item.replace(" ", "_")
            x = x.replace("-", "_")
            if x[0].isdigit():
                x = "_" + x
            new_column_names.append(x.lower())
        response.columns = new_column_names
        response = response[response.columns.drop(list(response.filter(regex='%_change_')))]

        # Change the types to float and removing all special characters
        for item in response.columns:
            if "conversion_value" in item:
                response[item] = response[item].replace({'£|€|$|>|<|,': ''}, regex=True)
                response[item] = response[item].astype(float)
            elif "_conversions" in item:
                response[item] = response[item].replace({'£|€|$|>|<|,': ''}, regex=True)
                response[item] = response[item].astype(float)

        clean_data = pd.DataFrame(response, columns=response.columns)

        clean_data["date"] = request_date
        if goal_management_data is not None:
            clean_data['conversion_id'] = [conversionId] * len(clean_data.index)
            clean_data['conversion_name'] = ['Ecommerce'] * len(clean_data.index)
        else:
            clean_data["conversion_ids"] = conversionId

        ga_attribution_data = pd.concat([ga_attribution_data, clean_data])

        #print(ga_attribution_data)

    return ga_attribution_data
'''


def big_query_push(data, creds_path, table_id):
    
    ##Load service account and set client
    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    
    ##Ensure that the schemas are correct
    field_data_types = pd.DataFrame(data=data.dtypes).reset_index()
    field_data_types.columns = ['field', 'format']
    big_query_schema = []
    for item in range(0,len(field_data_types)):
        x = field_data_types['field'][item]
        y = field_data_types['format'][item]
        if y == "object":
            big_query_schema.append(bigquery.SchemaField(x, "STRING"))
        elif y == "float64":
            big_query_schema.append(bigquery.SchemaField(x, "FLOAT"))
        elif y == "int64":
            big_query_schema.append(bigquery.SchemaField(x, "INTEGER"))
        else:
            big_query_schema.append(bigquery.SchemaField(x, "STRING"))
    
    ##Create and run the job
    job_config = bigquery.LoadJobConfig(schema=big_query_schema)
    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )
    
    # Wait for the load job to complete.

    return job.result()
      