import requests, pyautogui, time, datetime, random, os
import pandas as pd
from bs4 import BeautifulSoup
from random import randint
from time import sleep
import sys

# Create a log file and redirect the standard output to it
log_file = open("final3.log", "w")
sys.stdout = log_file

# Define the file paths to delete
file_paths = [
    "/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3_missed.xlsx",
    "/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3_missed2.xlsx",
    "/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3.xlsx"
    "/Users/jatinkashyap/Documents/Work/web_crawl/UCC/final3.log"
]
# Use a for loop to iterate through the file paths and delete each file
for file_path in file_paths:
    try:
        os.remove(file_path)
        print(f"File {file_path} has been deleted successfully!")
    except FileNotFoundError:
        print(f"File {file_path} does not exist!")
    except Exception as e:
        print(f"Error occurred while deleting file {file_path}: {e}")

# deleting file ends

#For loop starts
print("Starting For loop ...")
df = pd.DataFrame()
df_missed_ids = pd.DataFrame(columns=['missed_ids'])
##Setting User-Agent starts
user_agent_list = []
for i in range(5000):
    user_agent_list.append(f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/{random.randint(500, 600)}.36 (KHTML, like Gecko) Chrome/{random.randint(80, 90)}.0.{random.randint(4000, 5000)}.0 Safari/{random.randint(500, 600)}.36")
##Setting User-Agent ends

for url_var in range(1, 5700):
    try:
        wait_time = random.uniform(3, 6)
        print(f"Waiting for {wait_time/60:.1f} minutes before starting For loop iteration...")
        time.sleep(wait_time)
        print(url_var)
        user_agent = random.choice(user_agent_list) 
        headers = {'User-Agent': user_agent} 
        URL = f"https://oppsearch.ucc.org/web/fastdetails.aspx?id={url_var}&KeepThis=false&TB_iframe=true&height=798&width=960"
        page = requests.get(URL, headers=headers)
        #page = requests.get(URL, headers={'Cache-Control': 'no-cache'})
        soup = BeautifulSoup(page.content, "html.parser")
    
        ids = ["ContentPlaceHolder1_LocationNameLbl", "ContentPlaceHolder1_city", "ContentPlaceHolder1_state", 
           "ContentPlaceHolder1_ChurchSize", "ContentPlaceHolder1_PTorFT", "ContentPlaceHolder1_Duration",
           "ContentPlaceHolder1_positiontype", "ContentPlaceHolder1_Salary", "ContentPlaceHolder1_OtherBenefits",
           "ContentPlaceHolder1_Housing", "ContentPlaceHolder1_DatePosted", "ContentPlaceHolder1_lblStatus",
           "ContentPlaceHolder1_TinyURL"]
        results = []
        #del name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url
        for id in ids:
            result = soup.find(id=id).get_text()
            results.append(result)
        name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url = results
        size_int = int(size)
        data = {'church_name': name, 'church_city': city, 'church_state': state, 'church_size': size_int,
        'church_pt_or_ft': pt_or_ft, 'church_duration': duration, 'church_type_position': type_position,
        'church_salary_basis': salary_basis, 'church_benefits': benefits, 'church_housing': housing,
        'church_date_posted': date_posted, 'church_status': status, 'church_tiny_url': tiny_url}
        #df=df.append(data,ignore_index=True)
        df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
        #df=df.concat(data,ignore_index=True)
        #print(df)
        print(tiny_url)
        now = datetime.datetime.now()
        print(now)
        df.sort_values(by=['church_size']).to_excel(r'/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3.xlsx')
        del name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url
    
    except:
        wait_time = random.uniform(6, 9)
        print(f"Command failed, waiting for {wait_time/60:.1f} minutes before base_url touch...")
        time.sleep(wait_time)
        base_url = "https://oppsearch.ucc.org/web/fastsearch.aspx" #any link on website to trick the server & jump start it after failed attempt
        base_response = requests.get(base_url)
        #print(base_response.content)
        wait_time = random.uniform(5, 10)
        print(f"Command failed, waiting for {wait_time/60:.1f} minutes after base_url touch...")
        time.sleep(wait_time)
        #df_missed_ids = df_missed_ids.append({'missed_ids': url_var}, ignore_index=True)
        df_missed_ids = pd.concat([df_missed_ids, pd.DataFrame({'missed_ids': [url_var]})], ignore_index=True)
        df_missed_ids.to_excel('/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3_missed.xlsx', index=False)
        #del name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url    
        continue 
#For loop ends
# 5654 is last url ID active

###################### covering the missed values starts ######################
# Define a variable to store the URL
url_var = 0
# Define a variable to store the URL index
url_index = 0

print("*********************ENTERING INTO WHILE LOOP SEGMENT*********************")
# Use a while loop to iterate through the rows of the DataFrame
while not df_missed_ids.empty:
    # Get the URL index
    url_var = df_missed_ids.iloc[url_index, 0]

    # Execute a set of commands on url_var
    # If the commands are successful, delete the row
    try:
        # Execute commands on url_var
        # ...
        wait_time = random.uniform(3, 6); print(f"Waiting for {wait_time/60:.1f} minutes before starting While loop iteration...")
        time.sleep(wait_time); print(f"The value of url_var is: {url_var}"); print(f"The type of url_var is: {type(url_var)}") 
        user_agent = random.choice(user_agent_list); headers = {'User-Agent': user_agent} 
        URL = f"https://oppsearch.ucc.org/web/fastdetails.aspx?id={url_var}&KeepThis=false&TB_iframe=true&height=798&width=960"
        page = requests.get(URL, headers=headers)
        #page = requests.get(URL, headers={'Cache-Control': 'no-cache'})
        soup = BeautifulSoup(page.content, "html.parser")
        #print("Assign ids starting...!")
        ids = ["ContentPlaceHolder1_LocationNameLbl", "ContentPlaceHolder1_city", "ContentPlaceHolder1_state", 
           "ContentPlaceHolder1_ChurchSize", "ContentPlaceHolder1_PTorFT", "ContentPlaceHolder1_Duration",
           "ContentPlaceHolder1_positiontype", "ContentPlaceHolder1_Salary", "ContentPlaceHolder1_OtherBenefits",
           "ContentPlaceHolder1_Housing", "ContentPlaceHolder1_DatePosted", "ContentPlaceHolder1_lblStatus",
           "ContentPlaceHolder1_TinyURL"]; 
        #print("Assign ids end...!")   
        results = []
        #del name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url
        #print("Linking ids to results For loop starting...!")  
        for id in ids:
            result = soup.find(id=id).get_text()
            results.append(result)
        #print("Linking ids to results For loop end...!")  
        name, city, state, size, pt_or_ft, duration, type_position, salary_basis, benefits, housing, date_posted, status, tiny_url = results
        #print("Linked...!")  
        size_int = int(size)
        #print(f"The value of size is: {size}"); print(f"The type of size is: {type(size)}")
        #print("Int conversion starting...!")  
        #size_int = int(float(size))
        #print("Int conversion ends...!")  
        #print(f"The value of size_int is: {size_int}"); print(f"The type of size_int is: {type(size_int)}")
        data = {'church_name': name, 'church_city': city, 'church_state': state, 'church_size': size_int,
        'church_pt_or_ft': pt_or_ft, 'church_duration': duration, 'church_type_position': type_position,
        'church_salary_basis': salary_basis, 'church_benefits': benefits, 'church_housing': housing,
        'church_date_posted': date_posted, 'church_status': status, 'church_tiny_url': tiny_url}
        df=df.append(data,ignore_index=True); print(tiny_url)
        now = datetime.datetime.now(); print(now)
        df.sort_values(by=['church_size']).to_excel(r'/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3.xlsx')

        # Delete the row from the DataFrame
        df_missed_ids = df_missed_ids.drop(df_missed_ids.index[url_index])
        df_missed_ids.to_excel('/Users/jatinkashyap/Documents/Work/web_crawl/UCC/export_dataframe3_missed2.xlsx', index=False)
        print(f"Row {url_index} has been deleted successfully!")

    except Exception as e:
        print(f"Error occurred while processing row {url_index}: {e}")
        # Move to the next row
        url_index += 1
        # Jump back to the first row if the last row has been reached
        if url_index == len(df_missed_ids):
            url_index = 0
        wait_time = random.uniform(6, 9)
        print(f"Command failed, waiting for {wait_time/60:.1f} minutes before base_url touch in While loop...")
        time.sleep(wait_time)
        base_url = "https://oppsearch.ucc.org/web/fastsearch.aspx" #any link on website to trick the server & jump start it after failed attempt
        base_response = requests.get(base_url)
        print(base_response.content)
        wait_time = random.uniform(5, 10)
        print(f"Command failed, waiting for {wait_time/60:.1f} minutes after base_url touch in While loop...")
        time.sleep(wait_time)
print("All rows have been processed!")

###################### covering the missed values ends ######################
