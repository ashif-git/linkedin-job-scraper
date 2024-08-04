#!/usr/bin/env python3
import os
import re
import requests
import platform
import validators
import xlsxwriter
import pandas as pd
from time import sleep
from bs4 import BeautifulSoup

MESSAGE = """
    # LinkedIn Jobs Scraper, Dumps jobs in Excel
    # Running Python Version: {}
    # Require >= Python 3.12.0
    """

def http_request(url, url_descp=None, max_retries=6, delay=6, attempt=1):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp
    except (requests.HTTPError, requests.ConnectionError) as e:
        if attempt > max_retries:
            print(f"[#] - Max retries exceeded for {url_descp} URL: {url}")
            raise f"[{url_descp}]: {e}"
        else:
            print(f"[#] - Attempt {attempt} failed, {resp.status_code} Error Occurred. Sleeping for {delay} seconds, to Retry!")
            sleep(delay)
            print(f"[#] - Retrying the request: '{resp.url}'")
            return http_request(url, url_descp, max_retries, delay, attempt + 1)

def extract_emails(text):
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}"
    email_list_ = re.findall(email_regex, str(text))
    
    return "NA" if len(email_list_) == 0 else ",".join(email_list_)

def list_to_dict(data):
    if isinstance(data, list) == False:
        return {}
    
    itr = iter(data)
    dct = dict(zip(itr, itr))
    
    return dct
    
def get_jobs(url):
    try:
        resp = http_request(url, "Job ID")
    except Exception as e:
        print(e)
        return None
    
    """
    INFO: Extracting job-id data from the HTML response text
    -- Before tried tags --
    # data = soup.find("div", class_="show-more-less-html__markup relative overflow-hidden")
    -- -- -- -- -- -- -- -- 
    """
    soup = BeautifulSoup(resp.text, "html.parser")
    data = soup.find("div", class_="core-section-container__content break-words")
    if data == None:
        return None
    
    try:
        data_str = str(data.text)
        jb_rectr = str(data.find("h3", class_="base-main-card__title").text).strip()
        jb_descp = str(data.find("div", class_="show-more-less-html__markup").text).strip()
        jb_crita = str(data.find("ul", class_="description__job-criteria-list").text).strip().split("\n")
        jb_crita = list_to_dict([x.strip() for x in jb_crita if x.strip() != ""])
        jb_email = extract_emails(data_str)
    except (Exception, AttributeError) as e:
        jb_rectr = "NA"
        jb_descp = "NA"
        jb_crita = {}
        jb_email = "NA"
    
    job_data = {
        "Recruiter": jb_rectr,
        "Email": jb_email,
        "Link": resp.url
    } | jb_crita | {"Details": jb_descp.encode("utf-8")}
    
    return job_data
    
def scrap_jobs(num_page=1):
    job_list = []
    for page in range(num_page):
        try:
            url = f"https://www.linkedin.com/jobs/search/?keywords={keyword_}&location={location}&start={page * 25}"
            resp = http_request(url, "Job Query")
        except Exception as e:
            print(f"Request failed after maximum retries: {e}")
            continue
        
        """
        INFO: Extracting job posts from the HTML response text
        -- Before tried tags --
        # jobs = soup.find_all("ul", class_="jobs-search__results-list")
        # jobs = soup.find_all("div", class_="base-search-card__info")
        -- -- -- -- -- -- -- -- 
        """
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = soup.find_all(
            "div", 
            class_="base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card"
        )
        
        for job in jobs:
            jb_nam = job.find("h3", class_="base-search-card__title").text.strip()
            jb_cmp = job.find("h4", class_="base-search-card__subtitle").text.strip()
            jb_loc = job.find("span", class_="job-search-card__location").text.strip()
            jb_url = str(job.find("a", class_="base-card__full-link").get("href"))
            jb_dte = job.find("time", class_="job-search-card__listdate")
            
            """ -- Condition to collect, job post date if exist! -- """
            jb_dte = "NA" if (jb_dte == False or jb_dte == None) else jb_dte.get("datetime")
            
            """
            -- Condition to collect, job description detail from the job post id url. 
            -- If it was valid url!
            """
            jb_dsc = "NA" if validators.url(jb_url) == False else get_jobs(jb_url)
            
            """ -- [API-Query] Extracted HTML Dictionary -- """
            job_api_dict = {
                "PostedAt": jb_dte,
                "Title": jb_nam,
                "Company": jb_cmp,
                "Location": jb_loc
            }
            
            """ -- [JOB_ID-Link] Extracted HTML Dictionary -- """ 
            job_data_dict = {} if (jb_dsc == None or jb_dsc == "NA") else jb_dsc
            
            """ -- Merge Job-Query & Job-Id Data -- """
            merge_job_dt = job_api_dict | job_data_dict
            job_list.append(merge_job_dt)
            
            print('[*] - {}, {}, {}, {}, {}, {}...'.format(
                jb_dte, jb_nam, 
                jb_cmp, jb_loc, 
                merge_job_dt.get("Recruiter", "NA"), 
                merge_job_dt.get("Email", "NA")
            ))
            
    return job_list

def dump_sheet(result_set_list, file_name=f"job_list"):
    try:
        file_name = f"{file_name}" + "_".join(keyword_.split(" ")).strip()
        # Replaces any character with an underscore, that is not a (letter/digit/hyphen/underscore)
        file_name = re.sub(r"[^\w\-]", "_", file_name)
        # Limiting excel file name, 30 character as maximum
        file_name = file_name[:30] if len(file_name) > 30 else file_name
        
        exl_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), (f"{file_name}.xlsx"))
        exl_sheetname = "LINKEDIN_JOBS"
        
        df = pd.DataFrame(result_set_list)
        writer = pd.ExcelWriter(exl_filename, engine="xlsxwriter")
        df.to_excel(writer, sheet_name=exl_sheetname, index=False, na_rep="")
        
        workbook = writer.book
        worksheet = writer.sheets[exl_sheetname]
        worksheet_fmt = workbook.add_format({
            "font_name": "Cambria", 
            "bold": False, 
            "border": 1, 
            "border_color": "black"
        })
        
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets[exl_sheetname].set_column(col_idx, col_idx, column_length)
            
        worksheet.conditional_format(
            xlsxwriter.utility.xl_range(0, 0, len(df), len(df.columns)),
            {"type": "no_errors", "format": worksheet_fmt}
        )
        writer.close()
    except Exception as e:
        print(f"Uncaught exception has occurred, While processing excel: {e}")
        temp = f"{exl_filename}.temp_dict"
        with open(temp, "w", encoding="utf-8") as f:
            f.write(str(result_set_list))    
        
        print(f"Saved raw data as temp on path: '{temp}'")
        return temp
        
    return exl_filename

if __name__ == "__main__":
    print("================================================")
    print(MESSAGE.format(platform.python_version()))
    print("================================================")
    print("Start Your Job Scraping !!!")
    print(" i.e.,   SQL Developer")
    print("         Chennai, Tamil Nadu")
    print("   *** All the Best ***     ")
    print("================================================")
    keyword_ = str(input("Enter Searching Job Role: "))
    location = str(input("Enter Searching Job Location: "))
    print("================================================")
    num_page = 10
    jb_list_ = scrap_jobs(num_page)
    exl_path = dump_sheet(jb_list_)
    print(f"Saved {len(jb_list_)} job listings to '{exl_path}'")
