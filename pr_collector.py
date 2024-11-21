import argparse
import re
from datetime import datetime
from sys import stdout
import gspread
import requests
import yaml
from oauth2client.service_account import ServiceAccountCredentials
from slack_sdk import WebClient


class PrCollector:
   
    def __init__(self) -> None:
     
        # Collect the configurations
        with open('config.yml','r') as reader:
            self.creds_file = yaml.safe_load(reader)
            self.creds = self.creds_file['creds']['slack_bot_token']
            self.channel_id = self.creds_file['creds']['channel_id']
            self.google_sheet_token = self.creds_file['creds']['google_sheet_token']
            self.google_sheet_name = self.creds_file['creds']['google_sheet_name']
            self.google_worksheet_name = self.creds_file['creds']['google_worksheet_name']
            self.enterprise = self.creds_file['creds']['enterprise_url']
            self.organization = self.creds_file['creds']['organization']
            self.repo = self.creds_file['creds']['repo']
            self.github_token = self.creds_file['creds']['github_token']
            self.last_fetch_date = self.creds_file['creds']['slack_last_fetch_date']
            self.search_re_pattern = self.creds_file['creds']['search_re_pattern']
            
            
        # Connect to Slack
        self.slack_client = WebClient(token=self.creds)
        stdout.write('Connected to Slack API.\n')
        
        # Connect to Google Sheet
        self.scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(self.google_sheet_token, self.scope)
        self.sheet_client = gspread.authorize(self.creds)
        self.prworksheet = self.sheet_client.open(self.google_sheet_name)
        self.prsheet = self.prworksheet.worksheet(self.google_worksheet_name)
        self.all_rows = self.prsheet.get_all_values()
        stdout.write('Connected to Google Sheet API.\n')
        
        # Setup Github
        self.url = f'{self.enterprise}/repos/{self.organization}/{self.repo}/pulls'
        self.url_user = f'{self.enterprise}/users'
        self.headers = {'Authorization': f'token {self.github_token}'}
        stdout.write('Github Setup Complete.\n')

        

    
    def get_last_fetch_date(self):
        self.incremental_timestamp = self.last_fetch_date
        self.date_format = "%Y-%m-%d"
        self.date = datetime.strptime(self.incremental_timestamp, self.date_format)
        return int(self.date.timestamp())


 
    def slack_app(self):
       
        stdout.write('Collecting Pull Requests from Slack...\n')
        self.unix_timestamp = self.get_last_fetch_date()
        self.pr_list = []

        response = self.slack_client.conversations_history(channel=self.channel_id,oldest=self.unix_timestamp)
        slack_messages = response['messages']
         
        for index,message in enumerate(slack_messages):
           if f'{self.search_re_pattern}' in message['text']:  
                pr_link_list = re.findall(r'(<https.//github.*?)>', message['text'])
                
                for pr_link in pr_link_list:
                    pr_link_cleaned = re.sub(r'[<>]', '', pr_link)
                    if 'files' in pr_link_cleaned:
                        pr_index = pr_link_cleaned.rfind('/')
                        pr_link_cleaned = pr_link_cleaned[:pr_index]
                    self.pr_list.append(pr_link_cleaned)
           if index == 0:
               
               pr_last_fetch_date = datetime.fromtimestamp(int(float(message['ts']))).strftime('%Y-%m-%d')
               self.creds_file['creds']['slack_last_fetch_date'] = pr_last_fetch_date
               with open('config.yml','w') as writer_node:
                   yaml.dump(self.creds_file,writer_node)
        
        self.pr_list = sorted(list(set(self.pr_list)))


    def github_status_app(self):
          
          all_pr_links = [row[1] for row in self.all_rows] 
          
          stdout.write('Writing into Google Sheet...\n')
          for pr_link in self.pr_list:
              
              self.pr_number = pr_link.split('/')[-1]
              self.pr_url = f'{self.url}/{self.pr_number}'

              self.response = requests.get(self.pr_url, headers=self.headers)

              if self.response.status_code == 200:
                  
                  pr_data = self.response.json()
                  pr_title = pr_data['title']
                  pr_owner_id = pr_data['user']['login']
                  pr_owner_url = f'{self.url_user}/{pr_owner_id}'
                  user_response = requests.get(pr_owner_url,headers = self.headers)
                  pr_owner_name = user_response.json()['name']
                  pr_state = pr_data['state']
                  pr_merged = pr_data['merged']
                  pr_created_at = pr_data['created_at'].split('T')[0]
                  pr_merged_at = None
                  if pr_merged:
                      pr_state = 'merged'
                      pr_merged_at = pr_data['merged_at'].split('T')[0]
                  
                  if pr_link not in all_pr_links:
                       refined_pr = self.pr_url.replace('api/v3/repos/','').replace('pulls','pull')
                       self.prsheet.append_row([pr_title,refined_pr,pr_owner_name,pr_created_at,pr_state,pr_merged_at])

          stdout.write('Completed.\n')

        
    def git_merge_check(self):
        
        stdout.write('Checking Open Pull Requests...\n')

        open_rows = [(index,row) for index,row in enumerate(self.all_rows) if row[4]!='merged' and index!=0]

        

        for row in open_rows:
               pr_link = row[1][1]
               pr_index = row[0]
               self.pr_number = pr_link.split('/')[-1]
               self.pr_url = f'{self.url}/{self.pr_number}'
               self.response = requests.get(self.pr_url, headers=self.headers)
               if self.response.status_code == 200:
                  pr_merged = self.response.json()['merged']
                  pr_closed_or_draft_or_merged = self.response.json()['state']
                  if pr_merged:
                      pr_closed_or_draft_or_merged = 'merged'
                  self.prsheet.update_cell(pr_index+1, 5, pr_closed_or_draft_or_merged)

        stdout.write('Status Updated.\n')

    def process(self):

        # Collect Arguments 
        parser = argparse.ArgumentParser()
        parser.add_argument('--collect',action='store_true')
        parser.add_argument('--status',action='store_true')

        args = parser.parse_args()

        if args.collect:
            self.slack_app()
            self.github_status_app()
        
        elif args.status:
            self.git_merge_check()

        else: 
            stdout.write('Pass One Argument: --collect or --reset')





if __name__ == '__main__':
     pr_driver = PrCollector()
     pr_driver.process()