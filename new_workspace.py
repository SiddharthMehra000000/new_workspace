import requests
import pandas as pd
import pyarrow.parquet as pq
from currency_symbols import CurrencySymbols
import numpy as np
import import_ipynb
import ChartJS
import importlib
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt
​
​
class Main:
    def __init__(self, timePeriod, pathNames):
        self.data_period = timePeriod
        self.generateDataTest(pathNames)
        #         self.generateData(pathNames)
        self.charts = self.generateCharts()
​
    def generateDataTest(self, pathNames):
        self.campaignMeta = pq.read_pandas('goHigh5march/gohighMeta/campaign.parquet').to_pandas()
​
    def generateData(self, pathNames):
        self.campaignMeta = pd.DataFrame({})
        self.campaignMeta = pd.DataFrame({})
        self.campaignMeta = pd.DataFrame({})
        self.campaignMeta = pd.DataFrame({})
​
        try:
                self.campaignMeta = pq.read_pandas(pathNames[4]["filePath"]).to_pandas()
        except Exception as e:
                print(f'Error in filePaths {e}')
​
    def ms(self, x):
        epoch = datetime.utcfromtimestamp(0)
        milliseconds = (x - epoch).total_seconds() * 1000.0
        return milliseconds
​
    def generateCharts(self):
        chartsCont = []
        cardsCont = []
        time_period = f"<{self.data_period[0]} - {self.data_period[1]}>"
​
        campaignMeta = self.campaignMeta
       
        connect = []
        if len(campaignMeta) > 0:
            connect.append('Meta Ads')
​
        # meta int conversion
        try:
            campaignMeta['spend'] = campaignMeta['spend'].astype(float)
            campaignMeta['omni_purchase'] = np.random.randint(low=10, high=100, size=(len(campaignMeta)))
            campaignMeta['omni_purchase_value'] = np.random.randint(low=10000, high=100000, size=(len(campaignMeta)))
            campaignMeta['cost_per_conversion'] = np.random.randint(low=100, high=500, size=(len(campaignMeta)))
            campaignMeta['impressions'] = campaignMeta['impressions'].astype(float)
            campaignMeta['clicks'] = campaignMeta['clicks'].astype(float)
            campaignMeta['conversions'] = campaignMeta['conversions'].astype(float)
            campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date
​
        except Exception as e:
            print(f'Error in meta conversion {e}')
​
        try:
            currency_code = CurrencySymbols.get_symbol(campaignMeta['account_currency'].drop_duplicates().values[0])
        except:
            print("no data for currency code in meta")
​
​
        metaSpend = 0
        try:
            metaSpend = campaignMeta['spend'].sum().round(2)
            meta_conversion_value = campaignMeta['omni_purchase_value'].sum().round(2)
        except:
            print('Meta Calculation Error')


    def extract_Omni_Purchase_Conversions:

        # Extract omni_purchase conversions
        omni_purchase_conversions = []
        for actions_str in self.campaignMeta['actions']:
            actions = eval(actions_str)
            for action in actions:
                if action['action_type'] == 'omni_purchase':
                    omni_purchase_conversions.append(action['value'])
        # Extract omni_purchase conversion values
        omni_purchase_conversion_values = []
        for action_values_str in self.campaignMeta['action_values']:
            action_values = eval(action_values_str)
            omni_purchase_conversion_values.append(action_values['omni_purchase']
​
        # --------------------------------------------------------- Cards -----------------------------------------------------------


        ctr=campaignMeta['ctr'].mean()
        try:
            card1 = {
            'title': 'CTR',
            'overview': 'Overview',
            'formula': 'Clicks ÷ Impressions x 100',
            'unit': ['%'],
            'cardIdentifier': 1,
            'value': ctr
    }
            cardsCont.append(card_ctr)
​
        except Exception as e:
            print(f'Error in Card for CTR {e}')
​
        # Calculation of Conversion Rate
        conversion_rate = (campaignMeta['omni_purchase_conversion_values'].sum() / campaignMeta['impressions'].sum()) * 100
​
        # Card for Conversion Rate
        try:
            card2 = {
            'title': 'Conversion Rate',
            'overview': 'Overview',
            'formula': 'Omni-Channel Purchases ÷ Impressions x 100',
            'unit': ['%'],
            'cardIdentifier': 2,
            'value': conversion_rate
    }
            cardsCont.append(card_conversion_rate)
​
        except Exception as e:
            print(f'Error in Card for Conversion Rate {e}')
​
​
        Cost per conversion=campaignMeta['cost_per_conversion'].mean()
        try:
            card3 = {
            'title': 'Cost per Conversion',
            'overview': 'Overview',
            'formula': 'Total Ad Spend ÷ Conversions',
            'unit': ['USD'],
            'cardIdentifier': 3,
            'value': total_ad_spend / conversions
    }
            cardsCont.append(card3)
​
        except Exception as e:
            print(f'Error in Card for Cost per Conversion {e}')
​
​
        # Calculation of Spend
        spend = campaignMeta['spend'].sum()
​
        # Card for Spend
        try:
            card4 = {
            'title': 'Spend',
            'overview': 'Overview',
            'formula': 'Sum of Total Spend on Google Ads + Meta Ads',
            'unit': [currency_code],
            'cardIdentifier': 5,
            'value': spend
    }
            cardsCont.append(card_spend)
​
        except Exception as e:
            print(f'Error in Card for Spend {e}')
​
​        cpm=campaignMeta['cpm'].mean()
        # Card for CPM
        try:
            card5 = {
            'title': 'CPM',
            'overview': 'Overview',
            'formula': 'Total Spend on Meta Ads ÷ Impressions x 1000',
            'unit': [currency_code],
            'cardIdentifier': 4,
            'value': cpm
    }
            cardsCont.append(card_cpm)
​
        except Exception as e:
            print(f'Error in Card for CPM {e}')
​
        
        cpc=campaignMeta['cpc'].mean()
        # Card for CPC
        try:
            card6 = {
            'title': 'CPC',
            'overview': 'Overview',
            'formula': 'Total Spend on Google Ads ÷ Clicks',
            'unit': [currency_code],
            'cardIdentifier': 5,
            'value': cpc
    }
        cardsCont.append(card_cpc)
​
        except Exception as e:
            print(f'Error in Card for CPC {e}')
      
          # Chart 1: CTR vs Conversio Rate vs Ad Spend
    try:
      if metaSpend>0:

        metrics_df = campaignMeta.groupby('date').agg(
            {'impressions': 'mean', 'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df.sort_values(by='date')
        metrics_df['date'] = pd.to_datetime(metrics_df['date'])
        metrics_df['date'] = metrics_df['date'].apply(lambda x: self.ms(x))
        metrics_df = metrics_df[['date', 'impressions', 'clicks', 'conversions']]
        
        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": f'CTR',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": True,
            "y1AxisLabel": f'Conversion Rate vs Ad Spend',
            "xDate": True,
            "dualForAll": True,
            "connect": connect,
            "units": [currency_code, '%', currency_code],
            "formula": '''CTR = Total Clicks ÷ Total Impressions<br><br>
                          Conversion Rate = (Total Number of Conversions÷ Total number of sessions)*100<br><br>
                          Ad Spend = Amount Spent on Meta Ads'''
        }

        chart1 = ChartJS.MultiChartV2(merged, 'Trend in CTR, Conversion Rate, Ad Spend', 'line', 
                                      'Correlation of your CTR against conversion rate and ad spend', '', 
                                      '1', '1', config)

        chartsCont.append(chart1)
        del merged, chart1

        except Exception as e:
            print(f'Error in Chart 1 {e}')


   #Chart 2: Marketing Funnel Trend for impressions, clicks and conversions
    try:
      if metaSpend > 0:
        campaignMeta['impressions'] = campaignMeta['impressions'].asfloat()
        campaignMeta['clicks'] = campaignMeta['clicks'].asfloat()
        campaignMeta['conversions'] = campaignMeta['conversions'].asfloat()
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date

        metrics_df = campaignMeta.groupby('date').agg(
            {'impressions': 'mean', 'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df.sort_values(by='date')
        metrics_df['date'] = pd.to_datetime(metrics_df['date'])
        metrics_df['date'] = metrics_df['date'].apply(lambda x: self.ms(x))
        metrics_df = metrics_df[['date', 'impressions', 'clicks', 'conversions']]

        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": f'Impressions, clicks and converions ({currency_code})',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": True,
            "connect": connect,
            "units": [currency_code, currency_code, currency_code],
            "formula": '''Impressions = impressions<br><br>
                        Clicks = clicks<br><br>
                        Conversions = conversions''
        }

        chart2 = ChartJS.MultiChartV2(metrics_df, 'Trend in impressions, clicks and conversions', 'line',
                                              'Correlation of your impressions, clicks and conversions for Meta Ads',
                                              '',
                                              '2', '2', config)

        chartsCont.append(chart_combined)
        del metrics_df, chart_combined


        except Exception as e:
            print(f'Error in Chart 1 {e}')

    # Chart 3: CPC, CPM, Cost per Conversion
    try:
      if metaSpend > 0:
        campaignMeta['cpc'] = campaignMeta['cpc'].asfloat()
        campaignMeta['cpm'] = campaignMeta['spend'] / (campaignMeta['impressions'] / 1000)
        campaignMeta['cost_per_conversion'] = campaignMeta['spend'] / campaignMeta['conversions']
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date

        metrics_df = campaignMeta.groupby('date').agg(
            {'cpc': 'mean', 'cpm': 'mean', 'cost_per_conversion': 'mean'}).reset_index()

        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df.sort_values(by='date')
        metrics_df['date'] = pd.to_datetime(metrics_df['date'])
        metrics_df['date'] = metrics_df['date'].apply(lambda x: self.ms(x))
        metrics_df = metrics_df[['date', 'cpc', 'cpm', 'cost_per_conversion']]

        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": f'CPC, CPM and Cost per Conversion ({currency_code})',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": True,
            "connect": connect,
            "units": [currency_code, currency_code, currency_code],
            "formula": '''CPC = Total Spend on Meta Ads ÷ Number of Clicks on Meta Ads<br><br>
                        CPM = Total Spend on Meta Ads ÷ (Number of Impressions on Meta Ads ÷ 1000)<br><br>
                        Cost per Conversion = Total Spend on Meta Ads ÷ Number of Conversions from Meta Ads'''
        }

        chart3 = ChartJS.MultiChartV2(metrics_df, 'Trend in CPC, CPM and Cost per Conversion', 'line',
                                              'Correlation of your CPC, CPM, and Cost per Conversion for Meta Ads',
                                              '',
                                              '3', '3', config)

        chartsCont.append(chart_combined)
        del metrics_df, chart_combined

        except Exception as e:
            print(f'Error in Combined CPC, CPM, and Cost per Conversion Chart {e}')


    #Chart 4: Average Conversion Value and Cost per conversion
        # Chart Average Conversion Value and Cost per Conversion (Meta Ads only):
    try:
    if metaSpend > 0:
        campaignMeta['avg_conversion_value'] = campaignMeta['avg_conversion_value'].astype(float)
        campaignMeta['cost_per_conversion'] = campaignMeta['cost_per_conversion].asfloat()
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date

        metrics_df = campaignMeta.groupby('date').agg(
            {'avg_conversion_value': 'mean', 'cost_per_conversion': 'mean'}).reset_index()

        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df.sort_values(by='date')
        metrics_df['date'] = pd.to_datetime(metrics_df['date'])
        metrics_df['date'] = metrics_df['date'].apply(lambda x: self.ms(x))
        metrics_df = metrics_df[['date', 'avg_conversion_value', 'cost_per_conversion']]

        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": f'Average Conversion Value and Cost per Conversion ({currency_code})',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": True,
            "connect": connect,
            "units": [currency_code, currency_code],
            "formula": '''Average Conversion Value = Total Conversion Value from Meta Ads ÷ Number of Conversions from Meta Ads<br><br>
                        Cost per Conversion = Total Spend on Meta Ads ÷ Number of Conversions from Meta Ads'''
        }

        chart4 = ChartJS.MultiChartV2(metrics_df, 'Trend in Average Conversion Value and Cost per Conversion',
                                                'line',
                                                'Correlation of your Average Conversion Value and Cost per Conversion for Meta Ads',
                                                '',
                                                '4', '4', config)

        chartsCont.append(chart_conversion)
        del metrics_df, chart_conversion

        except Exception as e:
            print(f'Error in Average Conversion Value and Cost per Conversion Chart {e}')

    #Chart 5: Most Expensive Ad Campaigns
    try:
      if metaSpend > 0:
        campaignMeta['conversion_rate'] = (campaignMeta['omni_purchase_conversion_values'].sum() / campaignMeta['impressions'].sum()) * 100
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date


         # Select top 5 most expensive campaigns by spend
        top_expensive_campaigns = campaignMeta.nlargest(5, 'spend')['campaign_id'].unique()

        expensive_campaigns_df = campaignMeta[campaignMeta['campaign_id'].isin(top_expensive_campaigns)]
        expensive_campaigns_df = expensive_campaigns_df.groupby(['date', 'campaign_id']).agg({'conversion_rate': 'mean'}).reset_index()

        expensive_campaigns_df = expensive_campaigns_df.round(2)
        expensive_campaigns_df = expensive_campaigns_df.sort_values(by='date')
        expensive_campaigns_df['date'] = pd.to_datetime(expensive_campaigns_df['date'])
        expensive_campaigns_df['date'] = expensive_campaigns_df['date'].apply(lambda x: self.ms(x))

        # Pivot the dataframe to have a separate column for each campaign's conversion rate
        pivoted_df = expensive_campaigns_df.pivot_table(index='date', columns='campaign_id', values='conversion_rate').reset_index()

        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": True,
            "connect": connect,
            "units": ['%'],
            "formula": '''Conversion Rate = (Number of Conversions from Meta Ads ÷ Number of Clicks on Meta Ads) × 100'''
        }

        chart_conversion_rate = ChartJS.MultiChartV2(pivoted_df, 'Trend in Conversion Rate for Top 5 Most Expensive Ad Campaigns',
                                                     'line',
                                                     'Conversion Rate trend for the top 5 most expensive ad campaigns on Meta Ads',
                                                     '',
                                                     '1', '1', config)

        chartsCont.append(chart_conversion_rate)
        del expensive_campaigns_df, chart_conversion_rate

except Exception as e:
    print(f'Error in Conversion Rate for Most Expensive Ad Campaigns Chart {e}')

       


    #Chart 6 -CTR for Most Expensive Ad Campaigns
    try:
      if metaSpend > 0:
        campaignMeta['CTR'] = (campaignMeta['ctr'].asfloat()
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date

        # Select top 5 most expensive campaigns by spend
        top_expensive_campaigns = campaignMeta.nlargest(5, 'spend')['campaign_id'].unique()

        expensive_campaigns_df = campaignMeta[campaignMeta['campaign_id'].isin(top_expensive_campaigns)]
        expensive_campaigns_df = expensive_campaigns_df.groupby('date').agg({'CTR': 'mean'}).reset_index()

        expensive_campaigns_df = expensive_campaigns_df.round(2)
        expensive_campaigns_df = expensive_campaigns_df.sort_values(by='date')
        expensive_campaigns_df['date'] = pd.to_datetime(expensive_campaigns_df['date'])
        expensive_campaigns_df['date'] = expensive_campaigns_df['date'].apply(lambda x: self.ms(x))
        expensive_campaigns_df = expensive_campaigns_df[['date', 'CTR']]

        # Pivot the dataframe to have a separate column for each campaign's CTR
        pivoted_df = expensive_campaigns_df.pivot_table(index='date', columns='campaign_id', values='ctr').reset_index()

        config = {
            "xAxisLabel": 'Dates',
            "yAxisLabel": 'CTR',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": True,
            "connect": connect,
            "units": ['%'],
            "formula": '''CTR = (Number of Clicks on Meta Ads ÷ Number of Impressions on Meta Ads) × 100'''
        }

        chart6 = ChartJS.MultiChartV2(expensive_campaigns_df, 'Trend in CTR for Most Expensive Ad Campaigns',
                                         'line',
                                         'CTR trend for the top 10% most expensive ad campaigns on Meta Ads',
                                         '',
                                         '6', '6', config)

        chartsCont.append(chart_ctr)
        del expensive_campaigns_df, chart_ctr

        except Exception as e:
            print(f'Error in CTR for Most Expensive Ad Campaigns Chart {e}')


    #Chart 7 - Conversion Rate for most expensive ad campaigns
    try:
      if metaSpend > 0:
        campaignMeta['impressions'] = campaignMeta['impressions'].astype(float)
        campaignMeta['clicks'] = campaignMeta['clicks'].astype(float)
        campaignMeta['cost_per_unique_click'] = campaignMeta['cost_per_unique_click'].astype(float)
        campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date

        # Select top 5 most expensive campaigns by conversion rate
        top_expensive_campaigns = campaignMeta.nlargest(5, 'conversion_rate')['campaign_id'].unique()

        metrics_df = most_expensive_adsets.groupby('adset_name').agg(
            {'impressions': 'mean', 'clicks': 'mean', 'cost_per_unique_click': 'mean'}).reset_index()

        metrics_df['conversion_rate'] = metrics_df['clicks'] / metrics_df['impressions'] * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['adset_name', 'conversion_rate']]


        # Pivot the dataframe to have a separate column for each campaign's CTR
        pivoted_df = expensive_campaigns_df.pivot_table(index='date', columns='campaign_id', values='conversion_rate').reset_index()

        config = {
            "xAxisLabel": 'Ad Set Name',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''Conversion Rate = (Clicks / Impressions) * 100'''
        }

        chart7 = ChartJS.MultiChartV2(metrics_df, 'Conversion Rate of Most Expensive Ad Sets', 'bar',
                                                     'Conversion Rate for the 10 Most Expensive Ad Sets',
                                                     '',
                                                     '7', '7', config)

        chartsCont.append(chart_conversion_rate)
        del metrics_df, chart_conversion_rate

        except Exception as e:
            print(f'Error in Chart 1 {e}')

    #Chart 8 - CTR for most expensive ad sets
    try:
      if metaSpend > 0:
        adsetMeta['impressions'] = adsetMeta['impressions'].astype(float)
        adsetMeta['clicks'] = adsetMeta['clicks'].astype(float)
        adsetMeta['cost_per_unique_click'] = adsetMeta['cost_per_unique_click'].astype(float)
        adsetMeta['date'] = pd.to_datetime(adsetMeta['date_start']).dt.date

        # Sort by most expensive ad sets
        most_expensive_adsets = adsetMeta.sort_values(by='cost_per_unique_click', ascending=False).head(10)

        metrics_df = most_expensive_adsets.groupby('adset_name').agg(
            {'impressions': 'mean', 'clicks': 'mean', 'cost_per_unique_click': 'mean'}).reset_index()

        metrics_df['ctr'] = metrics_df['clicks'] / metrics_df['impressions'] * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['adset_name', 'ctr']]

        config = {
            "xAxisLabel": 'Ad Set Name',
            "yAxisLabel": 'CTR (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''CTR = (Clicks / Impressions) * 100'''
        }

        chart8 = ChartJS.MultiChartV2(metrics_df, 'CTR of Most Expensive Ad Sets', 'bar',
                                         'Click-Through Rate for the 10 Most Expensive Ad Sets',
                                         '',
                                         '8', '8', config)

        chartsCont.append(chart_ctr)
        del metrics_df, chart_ctr

        except Exception as e:
            print(f'Error in Chart 1 {e}')


    #Chart 9 - CTR by ad sets
    try:
      if metaSpend > 0:
         campaignMeta['impressions'] = campaignMeta['impressions'].astype(float)
          campaignMeta['clicks'] = campaignMeta['clicks'].astype(float)
          campaignMeta['conversions'] = campaignMeta['conversions'].astype(float)
            campaignMeta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date
        # Generate random ad positions for demonstration purposes
        adsetMeta['ctr'] = adsetMeta['ctr'].astypefloat()

        metrics_df = adsetMeta.groupby('ctr').agg(
            {'impressions': 'mean', 'clicks': 'mean'}).reset_index()

        metrics_df['ctr'] = metrics_df['clicks'] / metrics_df['impressions'] * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['ad_position', 'ctr']]

        config = {
            "xAxisLabel": 'Ad Position',
            "yAxisLabel": 'CTR (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''CTR = (Clicks / Impressions) * 100'''
        }

        chart9 = ChartJS.MultiChartV2(metrics_df, 'CTR by Ad Position', 'bar',
                                                     'Click-Through Rate by Ad Position',
                                                     '',
                                                     '9', '9', config)

        chartsCont.append(chart_ctr_by_position)
        del metrics_df, chart_ctr_by_position

        except Exception as e:
            print(f'Error in Chart 1 {e}')

    #Chart 10 - Conversion Rate by Ad Position
    try:
      if metaSpend > 0:
        
        adPlatform_Meta['conversions'] = adPlatform_Meta['conversions'].asfloat()
        atPlatform_Meta['platform_position'] = adPlatform_Meta['platform_position'].asfloat()

        metrics_df = adPlatform_Meta.groupby('platform_position').agg(
            {'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df['conversion_rate'] = metrics_df['conversions'] / metrics_df['clicks'] * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['platform_position', 'conversion_rate']]

        config = {
            "xAxisLabel": 'Ad Position',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''Conversion Rate = (Conversions / Clicks) * 100'''
        }

        chart10 = ChartJS.MultiChartV2(metrics_df, 'Conversion Rate by Ad Position', 'bar',
                                                            'Conversion Rate by Ad Position',
                                                            '',
                                                            '10', '10', config)

        chartsCont.append(chart_conversion_by_position)
        del metrics_df, chart_conversion_by_position

        except Exception as e:
            print(f'Error in Chart 1 {e}')

    #Chart 11 - Conversion Rate by Age Group
    try:
      if metaSpend > 0:
         adAge_Meta['impressions'] = adAge_Meta['impressions'].astype(float)
         adAge_Meta['clicks'] = adAge_Meta['clicks'].astype(float)
         adAge_Meta['conversions'] = adAge_Meta['conversions'].astype(float)
         adAge_Meta['date'] = pd.to_datetime(campaignMeta['date_start']).dt.date
        metrics_df = adAge_Meta.groupby('age').agg(
            {'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df['conversion_rate'] = metrics_df['conversions'] / metrics_df['clicks'] * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['age', 'conversion_rate']]

        config = {
            "xAxisLabel": 'Age Group',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''Conversion Rate = (Conversions / Clicks) * 100'''
        }

        chart11 = ChartJS.MultiChartV2(metrics_df, 'Conversion Rate by Age Group', 'bar',
                                                       'Conversion Rate by Age Group',
                                                       '',
                                                       '11', '11', config)

        chartsCont.append(chart_conversion_by_age)
        del metrics_df, chart_conversion_by_age

        except Exception as e:
            print(f'Error in Chart 1 {e}')

    #Chart 12 - CTR by Age Group
    try:
      if metaSpend > 0:
         adAge_Meta['impressions'] = adAge_Meta['impressions'].astype(float)
         adAge_Meta['clicks'] = adAge_Meta['clicks'].astype(float)
         adAge_Meta['conversions'] = adAge_Meta['conversions'].astype(float)
         adAge_Meta['date'] = pd.to_datetime(adAge_Meta['date_start']).dt.date

        metrics_df = adAge_Meta.groupby('age').agg(
            {'clicks': 'mean', 'impressions': 'mean'}).reset_index()

        metrics_df['ctr'] = (metrics_df['clicks'] / metrics_df['impressions']) * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['age', 'ctr']]

        config = {
            "xAxisLabel": 'Age Group',
            "yAxisLabel": 'CTR (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''CTR = (Clicks / Impressions) * 100'''
        }

        chart12 = ChartJS.MultiChartV2(metrics_df, 'CTR by Age Group', 'bar',
                                                'CTR by Age Group',
                                                '',
                                                '12', '12', config)

        chartsCont.append(chart_ctr_by_age)
        del metrics_df, chart_ctr_by_age

        except Exception as e:
            print(f'Error in Chart 1 {e}')

    #Chart 13 - Conversion Rate by Area
    try:
      if metaSpend > 0:
         adRegion_Meta['impressions'] = adRegion_Meta['impressions'].astype(float)
         adRegion_Meta['clicks'] = adRegion_Meta['clicks'].astype(float)
         adRegion_Meta['conversions'] = adRegion_Meta['conversions'].astype(float)
         adRegion_Meta['date'] = pd.to_datetime(adRegion_Meta['date_start']).dt.date
        metrics_df = adRegion_Meta.groupby('region').agg(
            {'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df['conversion_rate'] = (metrics_df['conversions'] / metrics_df['clicks']) * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['region', 'conversion_rate']]

        config = {
            "xAxisLabel": 'Region',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''Conversion Rate = (Conversions / Clicks) * 100'''
        }

        chart13 = ChartJS.MultiChartV2(metrics_df, 'Conversion Rate by Region', 'bar',
                                                                'Conversion Rate by Region',
                                                                '',
                                                                '13', '13', config)

        chartsCont.append(chart_conversion_rate_by_region)
        del metrics_df, chart_conversion_rate_by_region

        except Exception as e:
            print(f'Error in Chart 1 {e}')
    #Chart 14 - CTR vs Area
    try:
       adRegion_Meta['impressions'] = adRegion_Meta['impressions'].astype(float)
       adRegion_Meta['clicks'] = adRegion_Meta['clicks'].astype(float)
       adRegion_Meta['conversions'] = adRegion_['conversions'].astype(float)
       adRegion_Meta['date'] = pd.to_datetime(adRegion_Meta['date_start']).dt.date
      if metaSpend > 0:
        metrics_df = adRegion_Meta.groupby('region').agg(
            {'clicks': 'mean', 'conversions': 'mean'}).reset_index()

        metrics_df['conversion_rate'] = (metrics_df['conversions'] / metrics_df['clicks']) * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['region', 'conversion_rate']]

        config = {
            "xAxisLabel": 'Region',
            "yAxisLabel": 'Conversion Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": ['%', '%'],
            "formula": '''Conversion Rate = (Conversions / Clicks) * 100'''
        }

        chart14= ChartJS.MultiChartV2(metrics_df, 'Conversion Rate by Region', 'bar',
                                                                'Conversion Rate by Region',
                                                                '',
                                                                '14', '14', config)

        chartsCont.append(chart_conversion_rate_by_region)
        del metrics_df, chart_conversion_rate_by_region

        except Exception as e:
            print(f'Error in Chart 1 {e}')
            
    #Chart 15 - CTR vs Thru Play Videos
    try:
      if metaSpend > 0:
        campaignMeta['spend'] = campaignMeta['spend'].astype(float)
        campaignMeta['views'] = campaignMeta['views'].astype(float)
        campaignMeta['thru_plays'] = campaignMeta['thru_plays'].astype(float)

        metrics_df = campaignMeta.groupby(['video_id', 'video_title']).agg(
            {'spend': 'sum', 'views': 'sum', 'thru_plays': 'sum'}).reset_index()

        metrics_df['cpv'] = metrics_df['spend'] / metrics_df['views']
        metrics_df['thru_play_rate'] = (metrics_df['thru_plays'] / metrics_df['views']) * 100
        metrics_df = metrics_df.round(2)
        metrics_df = metrics_df[['video_title', 'cpv', 'thru_play_rate']]

        config = {
            "xAxisLabel": 'CPV',
            "yAxisLabel": 'ThruPlay Rate (%)',
            "timePeriod": time_period,
            "dataAlertLabel": '',
            "dualAxis": False,
            "xDate": False,
            "connect": connect,
            "units": [currency_code, '%'],
            "formula": '''CPV = Spend / Views<br><br>
                          ThruPlay Rate = (ThruPlays / Views) * 100'''
        }

        chart15= ChartJS.MultiChartV2(metrics_df, 'CPV vs ThruPlay Rate', 'scatter',
                                                     'CPV vs ThruPlay Rate for Videos',
                                                     '',
                                                     '15', '15', config)

        chartsCont.append(chart_cpv_vs_thruplay)
        del metrics_df, chart_cpv_vs_thruplay

        except Exception as e:
            print(f'Error in Chart 1 {e}')
        

------------#Reports------------------------------------------------------------



    try:
    config = {
    "timePeriod": time_period,
    "connect": connect,
    "formula": '''Impressions = Total number of times your ads were shown<br><br>
                    Clicks = Total number of clicks on your ads<br><br>
                    Conversions = Total number of conversions (e.g., purchases, sign-ups) driven by your ads<br><br>
                    Conversion Rate = Conversions ÷ Clicks<br><br>
                    CTR = Clicks ÷ Impressions<br><br>
                    Interaction Rate % = (Interactions ÷ Impressions) * 100<br><br>
                    Impression Share = The percentage of impressions that your ads received compared to the total number of impressions that your ads could get<br><br>
                    Cost per Conversion = Spend ÷ Conversions<br><br>
                    Spend = Total amount spent on the ads<br><br>
                    CPM = (Spend ÷ Impressions) * 1000<br><br>
                    CPC = Spend ÷ Clicks<br>'''
    }


    daily_data['Date'] = pd.to_datetime(daily_data['Date']).dt.date
    daily_data['Conversion Rate'] = (daily_data['Conversions'] / daily_data['Clicks']) * 100
    daily_data['CTR'] = (daily_data['Clicks'] / daily_data['Impressions']) * 100
    daily_data['Interaction Rate %'] = (daily_data['Interactions'] / daily_data['Impressions']) * 100
    daily_data['Cost per Conversion'] = daily_data['Spend'] / daily_data['Conversions']
    daily_data['CPM'] = (daily_data['Spend'] / daily_data['Impressions']) * 1000
    daily_data['CPC'] = daily_data['Spend'] / daily_data['Clicks']

    daily_data = daily_data.round(2)
    daily_data = daily_data.fillna(0)
    daily_data.replace([np.inf, -np.inf], 0, inplace=True)

    daily_data = daily_data.sort_values(by='Date', ascending=False)
    daily_data['Date'] = daily_data['Date'].dt.strftime('%d/%m/%Y')
    daily_data = daily_data.astype(str)

    table1 = ChartJS.Table(daily_data, 'Daily Performance Marketing Report',
                           'A daily report of your marketing performance including metrics such as impressions, clicks, conversions, and costs',
                           '', '1', '1, config, showFooter=True)

    chartsCont.append(chart9)
    del daily_data, chart9

    except Exception as e:
      print(f'Error in table 1{e}')





    try:

    config = {
    "timePeriod": time_period,
    "connect": connect,
    "formula": '''Impression = Total number of times your ads were shown<br><br>
                Impression Share (%) = The percentage of impressions that your ads received compared to the total number of impressions that your ads could get<br><br>
                % of Total Impressions = (Impression ÷ Total Impressions) * 100<br><br>
                Clicks = Total number of clicks on your ads<br><br>
                CTR (%) = (Clicks ÷ Impressions) * 100<br><br>
                Add-to-cart = Total number of times an item was added to the cart<br><br>
                Add-to-cart % = (Add-to-cart ÷ Clicks) * 100<br><br>
                Initiate Checkout = Total number of times the checkout process was initiated<br><br>
                Initiate Checkout % = (Initiate Checkout ÷ Add-to-cart) * 100<br><br>
                Conversions = Total number of conversions (e.g., purchases, sign-ups) driven by your ads<br><br>
                Thru-play = Number of times a video was played to the end<br><br>
                Conversion Rate (%) = (Conversions ÷ Initiate Checkout) * 100<br><br>
                Conversion Value (₹) = Total value of conversions<br><br>
                ROAS = Conversion Value (₹) ÷ Spend<br>'''
}


    campaign_data['Impression Share (%)'] = (campaign_data['Impression Share'] * 100)
    campaign_data['% of Total Impressions'] = (campaign_data['Impression'] / campaign_data['Impression'].sum()) * 100
    campaign_data['CTR (%)'] = (campaign_data['Clicks'] / campaign_data['Impression']) * 100
    campaign_data['Add-to-cart %'] = (campaign_data['Add-to-cart'] / campaign_data['Clicks']) * 100
    campaign_data['Initiate Checkout %'] = (campaign_data['Initiate Checkout'] / campaign_data['Add-to-cart']) * 100
    campaign_data['Conversion Rate (%)'] = (campaign_data['Conversions'] / campaign_data['Initiate Checkout']) * 100
    campaign_data['ROAS'] = (campaign_data['Conversion Value (₹)'] / campaign_data['Spend'])

    campaign_data = campaign_data.round(2)
    campaign_data = campaign_data.fillna(0)
    campaign_data.replace([np.inf, -np.inf], 0, inplace=True)


    ad_campaign_funnel = campaign_data[columns]


    table2 =chartJS.Table(ad_campaign_funnel, 'Ad Campaign Funnel Report',
                                     'A detailed report of the ad campaign funnel with key metrics')
    chartsCont.append(chart9)
    del daily_data, chart9

    except Exception as e:
      print(f'Error in table 2 {e}')




    try:
    config = {
        "timePeriod": time_period,
        "connect": connect,
        "formula": '''Total Spend = Total amount spent on the ads<br><br>
                    CPM = (Spend ÷ Impressions) * 1000<br><br>
                    CPC = Spend ÷ Clicks<br><br>
                    Cost per Conversion = Spend ÷ Conversions<br><br>
                    Cost per View = Spend ÷ Views<br><br>
                    RoAS = Return on Ad Spend = (Revenue ÷ Spend)<br><br>
                    % of Total Spend = (Campaign Spend ÷ Total Spend) * 100<br>'''
    }


    ad_campaign_data['CPM'] = (ad_campaign_data['Total Spend'] / ad_campaign_data['Impressions']) * 1000
    ad_campaign_data['CPC'] = ad_campaign_data['Total Spend'] / ad_campaign_data['Clicks']
    ad_campaign_data['Cost per Conversion'] = ad_campaign_data['Total Spend'] / ad_campaign_data['Conversions']
    ad_campaign_data['Cost per View'] = ad_campaign_data['Total Spend'] / ad_campaign_data['Views']
    ad_campaign_data['RoAS'] = ad_campaign_data['Revenue'] / ad_campaign_data['Total Spend']
    ad_campaign_data['% of Total Spend'] = (ad_campaign_data['Total Spend'] / ad_campaign_data['Total Spend'].sum()) * 100

    ad_campaign_data = ad_campaign_data.round(2)
    ad_campaign_data = ad_campaign_data.fillna(0)
    ad_campaign_data.replace([np.inf, -np.inf], 0, inplace=True)

    ad_campaign_data = ad_campaign_data.sort_values(by='Total Spend', ascending=False)
    ad_campaign_data = ad_campaign_data.astype(str)

    chart10 = ChartJS.Table(ad_campaign_data, 'Ad Campaign Spend Analysis Report',
                            'A report of your ad campaign performance including metrics such as total spend, CPM, CPC, cost per conversion, cost per view, RoAS, and % of total spend',
                            '', '10', '10', config, showFooter=True)

    chartsCont.append(chart10)
    del ad_campaign_data, chart10

    except Exception as e:
      print(f'Error in Chart 10 {e}')





    try:
    config = {
        "timePeriod": time_period,
        "connect": connect,
        "formula": '''Impressions = Total number of times your ads were shown<br><br>
                    Impression Share = The percentage of impressions that your ads received compared to the total number of impressions that your ads could get<br><br>
                    % of Total Impressions = (Impressions ÷ Total Impressions) * 100<br><br>
                    Clicks = Total number of clicks on your ads<br><br>
                    CTR = Clicks ÷ Impressions<br><br>
                    Add-to-cart = Total number of times a product is added to the cart<br><br>
                    Add-to-cart % = (Add-to-cart ÷ Clicks) * 100<br><br>
                    Initiate Checkout = Total number of times a user initiates checkout<br><br>
                    Initiate Checkout % = (Initiate Checkout ÷ Clicks) * 100<br><br>
                    Conversions = Total number of conversions (e.g., purchases, sign-ups) driven by your ads<br><br>
                    Thru-play = Total number of times your video ads were played to completion<br><br>
                    Conversion Rate = Conversions ÷ Clicks<br><br>
                    Conversion Value = Total revenue generated from conversions<br><br>
                    ROAS = (Conversion Value ÷ Ad Spend) * 100<br>'''
    }


    ad_group_data['% of Total Impressions'] = (ad_group_data['Impression'] / ad_group_data['Impression'].sum()) * 100
    ad_group_data['CTR'] = (ad_group_data['Clicks'] / ad_group_data['Impression']) * 100
    ad_group_data['Add-to-cart %'] = (ad_group_data['Add-to-cart'] / ad_group_data['Clicks']) * 100
    ad_group_data['Initiate Checkout %'] = (ad_group_data['Initiate Checkout'] / ad_group_data['Clicks']) * 100
    ad_group_data['Conversion Rate'] = (ad_group_data['Conversions'] / ad_group_data['Clicks']) * 100
    ad_group_data['ROAS'] = (ad_group_data['Conversion Value'] / ad_group_data['Ad Spend']) * 100

    ad_group_data = ad_group_data.round(2)
    ad_group_data = ad_group_data.fillna(0)
    ad_group_data.replace([np.inf, -np.inf], 0, inplace=True)

    ad_group_data = ad_group_data.sort_values(by='Ad Group name', ascending=True)
    ad_group_data = ad_group_data.astype(str)

    chart11 = ChartJS.Table(ad_group_data, 'Ad Group Funnel',
                            'A report of your ad group performance including metrics such as impressions, clicks, conversions, and ROAS',
                            '', '11', '11', config, showFooter=True)

    chartsCont.append(chart11)
    del ad_group_data, chart11

    except Exception as e:
      print(f'Error in Chart 11 {e}')





    try:
    config = {
        "timePeriod": time_period,
        "connect": connect,
        "formula": '''Total Spend = Total amount spent on the ads<br><br>
                    CPM = (Spend ÷ Impressions) * 1000<br><br>
                    CPC = Spend ÷ Clicks<br><br>
                    Cost per Conversion = Spend ÷ Conversions<br><br>
                    Cost per View = Spend ÷ Views<br><br>
                    Spend per Order = Spend ÷ Orders<br><br>
                    ROAS = (Revenue ÷ Spend) * 100<br><br>
                    % of Total Spend = (Spend ÷ Total Spend) * 100<br>'''
    }


    ad_group_spend_data['CPM'] = (ad_group_spend_data['Total Spend'] / ad_group_spend_data['Impressions']) * 1000
    ad_group_spend_data['CPC'] = ad_group_spend_data['Total Spend'] / ad_group_spend_data['Clicks']
    ad_group_spend_data['Cost per Conversion'] = ad_group_spend_data['Total Spend'] / ad_group_spend_data['Conversions']
    ad_group_spend_data['Cost per View'] = ad_group_spend_data['Total Spend'] / ad_group_spend_data['Views']
    ad_group_spend_data['Spend per Order'] = ad_group_spend_data['Total Spend'] / ad_group_spend_data['Orders']
    ad_group_spend_data['ROAS'] = (ad_group_spend_data['Revenue'] / ad_group_spend_data['Total Spend']) * 100
    ad_group_spend_data['% of Total Spend'] = (ad_group_spend_data['Total Spend'] / ad_group_spend_data['Total Spend'].sum()) * 100

    ad_group_spend_data = ad_group_spend_data.round(2)
    ad_group_spend_data = ad_group_spend_data.fillna(0)
    ad_group_spend_data.replace([np.inf, -np.inf], 0, inplace=True)

    ad_group_spend_data = ad_group_spend_data.sort_values(by='Ad group', ascending=True)
    ad_group_spend_data = ad_group_spend_data.astype(str)

    chart12 = ChartJS.Table(ad_group_spend_data, 'Ad Group Spend Analysis Report',
                            'A report of your ad group spend performance including metrics such as CPM, CPC, cost per conversion, cost per view, and ROAS',
                            '', '12', '12', config, showFooter=True)

    chartsCont.append(chart12)
    del ad_group_spend_data, chart12

    except Exception as e:
      print(f'Error in Chart 12 {e}')



    try:
    config = {
        "timePeriod": time_period,
        "connect": connect,
        "formula": '''Impressions = Total number of times your ads were shown<br><br>
                    Impression Share (%) = The percentage of impressions that your ads received compared to the total number of impressions that your ads could get<br><br>
                    % of Total Impressions = (Impressions ÷ Total Impressions) * 100<br><br>
                    Clicks = Total number of clicks on your ads<br><br>
                    CTR (%) = (Clicks ÷ Impressions) * 100<br><br>
                    Add-to-cart = Total number of items added to cart<br><br>
                    Add-to-cart % = (Add-to-cart ÷ Clicks) * 100<br><br>
                    Initiate Checkout = Total number of initiated checkouts<br><br>
                    Initiate Checkout % = (Initiate Checkout ÷ Clicks) * 100<br><br>
                    Conversions = Total number of conversions (e.g., purchases, sign-ups) driven by your ads<br><br>
                    Thru-play = Total number of video plays completed<br><br>
                    Conversion Rate (%) = (Conversions ÷ Clicks) * 100<br><br>
                    Conversion Value (₹) = Total value of conversions<br><br>
                    ROAS = (Conversion Value ÷ Ad Spend) * 100<br>'''
    }

    ad_funnel_data['% of Total Impressions'] = (ad_funnel_data['Impression'] / ad_funnel_data['Impression'].sum()) * 100
    ad_funnel_data['CTR (%)'] = (ad_funnel_data['Clicks'] / ad_funnel_data['Impression']) * 100
    ad_funnel_data['Add-to-cart %'] = (ad_funnel_data['Add-to-cart'] / ad_funnel_data['Clicks']) * 100
    ad_funnel_data['Initiate Checkout %'] = (ad_funnel_data['Initiate Checkout'] / ad_funnel_data['Clicks']) * 100
    ad_funnel_data['Conversion Rate (%)'] = (ad_funnel_data['Conversions'] / ad_funnel_data['Clicks']) * 100
    ad_funnel_data['ROAS'] = (ad_funnel_data['Conversion Value (₹)'] / ad_funnel_data['Ad Spend']) * 100

    ad_funnel_data = ad_funnel_data.round(2)
    ad_funnel_data = ad_funnel_data.fillna(0)
    ad_funnel_data.replace([np.inf, -np.inf], 0, inplace=True)

    ad_funnel_data = ad_funnel_data.sort_values(by='Ad name', ascending=True)
    ad_funnel_data = ad_funnel_data.astype(str)

    chart13 = ChartJS.Table(ad_funnel_data, 'Ad Funnel Report',
                            'A report of your ad funnel performance including metrics such as impressions, clicks, conversions, and ROAS',
                            '', '13', '13', config, showFooter=True)

    chartsCont.append(chart13)
    del ad_funnel_data, chart13

    except Exception as e:
      print(f'Error in Chart 13 {e}')



    try:
    config = {
        "timePeriod": time_period,
        "connect": connect,
        "formula": '''Total Spend = Total amount spent on the ads<br><br>
                    CPM = (Spend ÷ Impressions) * 1000<br><br>
                    CPC = Spend ÷ Clicks<br><br>
                    Cost per Conversion = Spend ÷ Conversions<br><br>
                    Cost per View = Spend ÷ Views<br><br>
                    Spend per Order = Spend ÷ Orders<br><br>
                    ROAS = (Revenue ÷ Spend) * 100<br><br>
                    % of Total Spend = (Spend ÷ Total Spend) * 100<br>'''
    }


    ad_spend_data['CPM'] = (ad_spend_data['Total Spend'] / ad_spend_data['Impressions']) * 1000
    ad_spend_data['CPC'] = ad_spend_data['Total Spend'] / ad_spend_data['Clicks']
    ad_spend_data['Cost per Conversion'] = ad_spend_data['Total Spend'] / ad_spend_data['Conversions']
    ad_spend_data['Cost per View'] = ad_spend_data['Total Spend'] / ad_spend_data['Views']
    ad_spend_data['Spend per Order'] = ad_spend_data['Total Spend'] / ad_spend_data['Orders']
    ad_spend_data['ROAS'] = (ad_spend_data['Revenue'] / ad_spend_data['Total Spend']) * 100
    ad_spend_data['% of Total Spend'] = (ad_spend_data['Total Spend'] / ad_spend_data['Total Spend'].sum()) * 100

    ad_spend_data = ad_spend_data.round(2)
    ad_spend_data = ad_spend_data.fillna(0)
    ad_spend_data.replace([np.inf, -np.inf], 0, inplace=True)

    ad_spend_data = ad_spend_data.sort_values(by='Ad Name', ascending=True)
    ad_spend_data = ad_spend_data.astype(str)

    chart14 = ChartJS.Table(ad_spend_data, 'Ad Level Spend Analysis Report',
                            'A report of your ad level spend analysis including metrics such as CPM, CPC, cost per conversion, and ROAS',
                            '', '14', '14', config, showFooter=True)

    chartsCont.append(chart14)
    del ad_spend_data, chart14

    except Exception as e:
      print(f'Error in Chart 14 {e}')












