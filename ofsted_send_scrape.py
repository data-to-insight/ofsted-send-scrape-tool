#
# Export options

export_summary_filename = 'ofsted_csc_send_overview'
d2i_contact_email = "datatoinsight.enquiries@gmail.com"

# export_file_type         = 'csv' # Excel / csv currently supported
export_file_type         = 'excel'

# Default (sub)folder structure
# Defined to offer some ease of onward flexibility

# data exports
root_export_folder = 'export_data'              # <all> exports folder
inspections_subfolder = 'inspection_reports'    # downloaded report pdfs

# data imports
import_la_data_path = 'import_data/la_lookup/'
import_geo_data_path = 'import_data/geospatial/'
geo_boundaries_filename = 'local_authority_districts_boundaries.json'

# scrape inspection grade/data from pdf reports
pdf_data_capture = True # True is default (scrape within pdf inspection reports for inspection results etc)
                        # This impacts run time E.g False == ~1m20 / True == ~ 4m10
                        # False == only pdfs/list of LA's+link to most recent exported. Not inspection results.



#
# Ofsted site/page admin settings


# Define max results per page (Now limited to 100)
max_page_results = 100  # new ofsted search limit at w/c 100225
url_stem = 'https://reports.ofsted.gov.uk/'

# Base search URL (excluding pagination controls)
search_url = 'search?q=&location=&lat=&lon=&radius=&level_1_types=3&level_2_types%5B%5D=12'

# pagination params placehold
pagination_param = '&start={start}&rows=' + str(max_page_results)

start = 0
max_results = 160  # expecting 153 @110225



# #
# # In progress Ofsted site/search link refactoring

# search_category = 3         # Default 3  == 'Childrens social care' (range 1 -> 4)
# search_sub_category = 12    # Default 12 == 'Local Authority Childrens Services' (range 8 -> 12)


#
# Script admin settings

# Standard library imports
import os
import io
import re
import json
from datetime import datetime, timedelta

# Third-party library imports
import requests
import git # possible case for just: from git import Repo
from requests.exceptions import RequestException
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta


# pdf search/data extraction
try:
    import fitz  # PyMuPDF
    import tabula  
    import PyPDF2  
except ModuleNotFoundError:
    print("Please install 'tabula-py' and 'PyPDF2' using pip")


# handle optional excel export+active file links
try:
    import xlsxwriter
except ModuleNotFoundError:
    print("Please install 'openpyxl' and 'xlsxwriter' using pip")



# Configure logging/logging module
import warnings
import logging

# wipe / reset the logging file 
with open('output.log', 'w'):
    # comment out if maintaining ongoing/historic log
    pass

# Keep warnings quiet unless priority
logging.getLogger('org.apache.pdfbox').setLevel(logging.ERROR)
warnings.filterwarnings('ignore')

logging.basicConfig(filename='output.log', level=logging.INFO, format='%(asctime)s - %(message)s')



# Needed towards git actions workflow
# Use GITHUB_WORKSPACE env var(str) if available(workflow actions), 
# otherwise fall back to the default path(codespace).
repo_path = os.environ.get('GITHUB_WORKSPACE', '/workspaces/ofsted-jtai-scrape-tool')
print("Using repo path:", repo_path)

try:
    # repo object using path string
    repo = git.Repo(repo_path)
except git.exc.NoSuchPathError:
    print(f"Error initialising repo path for inspection reports: {repo_path}")
    raise



#
# Function defs

def get_soup(url, retries=3, delay=5):
    """
    Given a URL, returns a BeautifulSoup object + request error handling
    Args:
        url (str):      The URL to fetch and parse
        retries (int):  Number of retries on network errors
        delay (int):    Delay between retries in seconds
    Returns:
        BeautifulSoup: The parsed HTML content, or None if an error occurs
    """
    timeout_seconds = 10  # lets not assume the Ofsted page is up, avoid over-pinging

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout_seconds)
            response.raise_for_status()  # any HTTP errors?
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
        except Timeout:
            print(f"Timeout getting URL '{url}' on attempt {attempt + 1}. Retrying after {delay} secs...")
            time.sleep(delay)
        except HTTPError as e:
            print(f"HTTP error getting URL '{url}': {e}")
            return None  # end retries on client and server errors
        except RequestException as e:
            print(f"Request error getting URL '{url}': {e}")
            if attempt < retries - 1:
                print(f"Retrying after {delay} secs...")
                time.sleep(delay) # pause to assist not getting blocked
            else:
                print("Max rtry attempts reached, giving up")
                return None
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            return None

    return None  # All the retries failed / stop point


def clean_provider_name(name):
    """
    Cleans the la/provider name according to:
                - expected output based on existing ILACS sheet
                - historic string issues seen on Ofsted site

    Args:
        name (str): The original name to be cleaned.
    Returns:
        str: The cleaned name.
    """
    # Convert to lowercase and remove extra spaces
    name = name.lower().replace('  ', ' ')
    
    # Remove specific phrases
    name = name.replace("royal borough of ", "").replace("city of ", "").replace("metropolitan district council", "").replace("london borough of", "").replace("council of", "")

    # Remove further undesired 'single' words and join the remaining parts
    name_parts = [part for part in name.split() if part not in ['city', 'metropolitan', 'borough', 'council', 'county', 'district', 'the']] 
    return ' '.join(name_parts)




## Need to refactor the below funcs. Lots of duplication going on
def format_date(date_str: str, input_format: str, output_format: str) -> str:
    """
    Convert and format a date string.

    Args:
        date_str (str): The input date string.
        input_format (str): The format of the input date string.
        output_format (str): The desired output format.

    Returns:
        str: The formatted date string.
    """
    dt = datetime.strptime(date_str, input_format)
    date_obj = dt.date()

    return date_obj.strftime(output_format)


def parse_date(date_str, date_format):
    try:
        dt = datetime.strptime(date_str, date_format)
        return dt.date()  # only need date 
    except (TypeError, ValueError):
        return None
    

def format_date_for_report_BAK(date_obj, output_format_str):
    """
    Formats a datetime object as a string in the d/m/y format, or returns an empty string if the input is None.

    Args:
        date_obj (datetime.datetime or None): The datetime object to format, or None.

    Returns:
        str: The formatted date string, or an empty string if date_obj is None.
    """
    if date_obj is not None:
        return date_obj.strftime(output_format_str)
    else:
        return ""
    
def format_date_for_report(date_input, output_format_str, input_format_str=None):
    """
    Formats a datetime object or a date string as a string in the specified format, or returns an empty string if the input is None.

    Args:
        date_input (datetime.datetime, str, or None): The datetime object or date string to format, or None.
        output_format_str (str): The desired output format for the date string.
        input_format_str (str, optional): The format to use for parsing the input date string, if date_input is a string.

    Returns:
        str: The formatted date string, or an empty string if date_input is None.
    """
    if date_input is None:
        return ""

    if isinstance(date_input, str):
        date_obj = None
        if input_format_str:
            try:
                date_obj = datetime.strptime(date_input, input_format_str)
            except ValueError:
                raise ValueError(f"Report date format for {date_input} does not match {input_format_str}")
        else:
            # Try common date formats including two-digit yrs
            formats = ["%d %B %Y", "%d/%m/%Y", "%d/%m/%y"]
            for fmt in formats:
                try:
                    date_obj = datetime.strptime(date_input, fmt)
                    break
                except ValueError:
                    continue
            if date_obj is None:
                raise ValueError(f"Report date format for {date_input} is not supported")
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise TypeError("Report date_input must be a datetime object, a string, or None")

    return date_obj.strftime(output_format_str)

## Need to refactor the above funcs. Lots of duplication going on



def extract_dates_from_text(text):
    """
    Extracts and cleans inspection dates from the given text.
    This has heavy outputs atm due to multiple problem report formats and ongoing testing

    Args:
    text (str): The text from which to extract dates.

    Returns:
    tuple: A tuple containing the start and end dates as strings in the format 'dd/mm/yy'.

    Notes:
    # Some clean up based on historic data obs from scraped reports/incl. ILACS
        # Ofsted reports contain inspection date strings in multiple formats (i/ii/iii...)
        #   i)      "15 to 26 November"  
        #   ii)     "28 February to 4 March" or "8 October to 19 October" (majority)
        #   iii)    ['8 July ', '12 July   and 7 August  to'] (*recently seen)
        #   iv)     "11 September 2017 to 5 October 2017" (double year)
        #   v)      "Inspection dates: 19 November–30 November 2018" (Bromley)
        #   vi)     white spaces between date numbers e.g. "wiltshire,	1 9 June 2019"
        #   vii)    'None' years where no recognisable was found
    """
    # print("Debug: Starting date extraction")

    if not text:
        print("Debug: Input text is empty or None.")
        raise ValueError("No text provided")

    # Remove non-printing characters and multiple spaces
    cleaned_text = re.sub(r'[^\x20-\x7E]', '', text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)

    # Preprocess the inspection_dates to fix split years, e.g. 20 23, 20 24 -> 2023, 2024
    cleaned_text = re.sub(r"(\b20)\s+(\d{2}\b)", r"\1\2", cleaned_text)
    #print(f"Debug: Cleaned text: {cleaned_text}")




    # Try to capture date ranges correctly
    # date_match = re.search(r"Inspection dates:\s*(.+?)(?=\s{2,}|$)", cleaned_text) - doesnt work for oxfordshire
    # date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2}(?: to \d{1,2})? \w+ \d{4})", cleaned_text)

    # # Not implemented. But in case need to handle cases of repeating year alongside known repeating month "13 July 2023 to 21 July 2023" e.g. West Sussex
    # date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2}(?: \w+ \d{4})?(?: to \d{1,2})? \w+ \d{4})", cleaned_text)
    date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2} \w+ \d{4}) to (\d{1,2} \w+ \d{4})", cleaned_text)


    if date_match:
        #print(f"Debug: Primary date match found: {date_match.group(0)}")
        # Extract start and end dates directly from the match
        start_date_str = date_match.group(1).strip()
        end_date_str = date_match.group(2).strip()
    else:
        #print("Debug: Primary date match not found, trying fallback method")
        # Fallback to capturing single date or simpler range within the same month
        date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2}) to (\d{1,2}) (\w+) (\d{4})", cleaned_text)

        if date_match:
            #print(f"Debug: Fallback date match found: {date_match.group(0)}")
            start_day = date_match.group(1)
            end_day = date_match.group(2)
            month = date_match.group(3)
            year = date_match.group(4)

            start_date_str = f"{start_day} {month} {year}"
            end_date_str = f"{end_day} {month} {year}"

        else:

            raise ValueError(f"Extract_dates_from_text - No inspection dates found: {cleaned_text}")

    # Clean and format the extracted dates
    try:
        start_date = datetime.strptime(start_date_str, "%d %B %Y").strftime("%d/%m/%y")
        end_date = datetime.strptime(end_date_str, "%d %B %Y").strftime("%d/%m/%y")
        #print(f"Debug: Formatted start date: {start_date}")
        #print(f"Debug: Formatted end date: {end_date}")
    except ValueError as ve:
        print(f"Error converting date: {ve}")
        raise ValueError("Date conversion failed")

    # Now handle previous inspection dates if present in the same cleaned_text
    previous_inspection_match = re.search(r"Dates? of previous inspection:\s*(\d{1,2}) to (\d{1,2}) (\w+) (\d{4})", cleaned_text)
    if previous_inspection_match:
        #print(f"Debug: Previous inspection match found: {previous_inspection_match.groups()}")
        previous_start_day = previous_inspection_match.group(1)
        previous_end_day = previous_inspection_match.group(2)
        previous_month = previous_inspection_match.group(3)
        previous_year = previous_inspection_match.group(4)

        previous_end_date_str = f"{previous_end_day} {previous_month} {previous_year}"

        try:
            previous_end_date = datetime.strptime(previous_end_date_str, "%d %B %Y").strftime("%d/%m/%Y")
            #print(f"Debug: Formatted previous inspection end date: {previous_end_date}")
        except ValueError as ve:
            print(f"Error converting previous inspection date: {ve}")
            previous_end_date = "01/01/1900"  # Placeholder date for conversion errors
    else:
        #print("Debug: No previous inspection date found, using placeholder.")
        previous_end_date = "01/01/1900"  # Placeholder date if no match found

    # Final debug print to verify results
    print(f"\nStart Date: {start_date}, End Date: {end_date}, Previous Inspection End Date: {previous_end_date}")
    
    return start_date, end_date, previous_end_date



# # revised version with improved date handling for fallback cases previously not being picked up in main inspection date(s)
# # this added processing then added also to previous inspection date handling in the newer below version
# def extract_dates_from_text(text):
#     if not text:
#         raise ValueError("No text provided")

#     # Remove non-printing characters and multiple spaces
#     cleaned_text = re.sub(r'[^\x20-\x7E]', '', text)
#     cleaned_text = re.sub(r'\s+', ' ', cleaned_text)

#     # Fix split years (e.g., 20 23 -> 2023)
#     cleaned_text = re.sub(r"(\b20)\s+(\d{2}\b)", r"\1\2", cleaned_text)

#     # Primary regex: Both start and end dates include the year
#     date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2} \w+ \d{4}) to (\d{1,2} \w+ \d{4})", cleaned_text)

#     if date_match:
#         start_date_str = date_match.group(1).strip()
#         end_date_str = date_match.group(2).strip()

#     else:
#         # **Fallback 1:** Year appears only once, and months **may be different**
#         date_match = re.search(r"Inspection dates\s*:\s*(\d{1,2}) (\w+) to (\d{1,2}) (\w+) (\d{4})", cleaned_text)

#         if date_match:
#             start_day = date_match.group(1)
#             start_month = date_match.group(2)
#             end_day = date_match.group(3)
#             end_month = date_match.group(4)
#             year = date_match.group(5)

#             start_date_str = f"{start_day} {start_month} {year}"
#             end_date_str = f"{end_day} {end_month} {year}"

#         else:
#             # **Fallback 2:** Single inspection date
#             date_match = re.search(r"Inspection dates?\s*:\s*(\d{1,2} \w+ \d{4})", cleaned_text)

#             if date_match:
#                 start_date_str = date_match.group(1).strip()
#                 end_date_str = start_date_str  # If only one date is mentioned, assume it's the same
#             else:
#                 raise ValueError(f"Extract_dates_from_text - Could not identify inspection dates in: {cleaned_text}")

#     # Convert extracted dates to dd/mm/yy format
#     try:
#         start_date = datetime.strptime(start_date_str, "%d %B %Y").strftime("%d/%m/%y")
#         end_date = datetime.strptime(end_date_str, "%d %B %Y").strftime("%d/%m/%y")
#     except ValueError as ve:
#         raise ValueError(f"Date conversion failed: {ve}")

#     # Handling previous inspection dates
#     previous_inspection_match = re.search(r"Dates? of previous inspection:\s*(\d{1,2}) to (\d{1,2}) (\w+) (\d{4})", cleaned_text)

#     if previous_inspection_match:
#         previous_start_day = previous_inspection_match.group(1)
#         previous_end_day = previous_inspection_match.group(2)
#         previous_month = previous_inspection_match.group(3)
#         previous_year = previous_inspection_match.group(4)

#         previous_end_date_str = f"{previous_end_day} {previous_month} {previous_year}"

#         try:
#             previous_end_date = datetime.strptime(previous_end_date_str, "%d %B %Y").strftime("%d/%m/%Y")
#         except ValueError:
#             previous_end_date = "01/01/1900"
#     else:
#         previous_end_date = "01/01/1900"

#     print(f"\nStart Date: {start_date}, End Date: {end_date}, Previous Inspection End Date: {previous_end_date}")
    
#     return start_date, end_date, previous_end_date






def extract_inspection_data_update(pdf_content):
    """
    Function to extract key details from inspection reports PDF.

    Args:
        pdf_content (bytes): The raw content of the PDF file to be processed. 

    Returns:
        dict: A dictionary containing the extracted details. The dictionary keys are as follows:
            - 'table_rows_found': Number of rows found in the table.
            - 'inspector_name': The name of the inspector.
            - 'overall_inspection_grade': The overall effectiveness grade.
            - 'inspection_start_date': The start date of the inspection.
            - 'inspection_end_date': The end date of the inspection.
            - 'inspection_framework': The inspection framework string.
            - 'impact_of_leaders_grade': The impact of leaders grade.
            - 'help_and_protection_grade': The help and protection grade.
            - 'in_care_grade': The in care grade.
            - 'care_leavers_grade': The care leavers grade.
            - 'sentiment_score': The sentiment score of the inspection report.
            - 'sentiment_summary': The sentiment summary of the inspection report.
            - 'main_inspection_topics': List of key inspection themes.
    
    Raises:
        ValueError: If the PDF content is not valid or cannot be processed correctly.
        
    Note:
        This function expects the input PDF to contain specific sections specifically
        the inspection judgements to be on page 1 (page[0]) 
        If the PDF structure is different, obv the function will need changing. 
    """

    # Create a file-like buffer for the PDF content
    with io.BytesIO(pdf_content) as buffer:
        # Read the PDF content for text extraction
        reader = PyPDF2.PdfReader(buffer)
        
        # Extract the first page of inspection report pdf
        first_page_text = reader.pages[0].extract_text()

        # Not needed in SEND extract(yet) - at least not for overview summary
        # # Extract text from <all> pages in the pdf
        # full_text = ''
        # for page in reader.pages:
        #     full_text += page.extract_text()

    #   # Carry over for ref from ILACS. Not used in SEND
    #     # Find the inspector's name using a regular expression
    #     match = re.search(r"Lead inspector:\s*(.+)", first_page_text)
    #     if match:
    #         inspector_name = match.group(1)
            
    #         inspector_name = inspector_name.split(',')[0].strip()       # Remove everything after the first comma (some contain '.., Her Majesty’s Inspector')
    #         inspector_name = inspector_name.replace("HMI", "").rstrip() # Remove "HMI" and any trailing spaces(some inspectors add this to name)

    #     else:
    #         inspector_name = None


    # remove all non-printing chars from text content
    first_page_text= re.sub(r'[^\x20-\x7E]', '', first_page_text)

    # extract and format inspection dates
    try:
        # Attempt to extract and format dates
        start_date_formatted, end_date_formatted, previous_inspection_date = extract_dates_from_text(first_page_text)
        
        # Validate the start date
        try:
            datetime.strptime(start_date_formatted, "%d/%m/%y")
        except (ValueError, TypeError) as e:
            print(f"Error with start date: {e}")
            start_date_formatted = None
        
        # Validate the end date
        try:
            datetime.strptime(end_date_formatted, "%d/%m/%y")
        except (ValueError, TypeError) as e:
            print(f"Error with end date: {e}")
            end_date_formatted = None
        
        # Validate the previous inspection date
        try:
            datetime.strptime(previous_inspection_date, "%d/%m/%Y")
        except (ValueError, TypeError) as e:
            print(f"Error with previous inspection date: {e}")
            previous_inspection_date = None

    except ValueError as e:
        # If there was a broader issue with the extraction function itself
        start_date_formatted = None
        end_date_formatted = None
        previous_inspection_date = None
        print(f"Error: {e}")

        

        # end test block


    return {
        # main inspection details
        # 'inspector_name':           inspector_name, 
        # 'overall_inspection_grade': inspection_grades_dict['overall_effectiveness'],
        'inspection_start_date':    start_date_formatted,
        'inspection_end_date':      end_date_formatted,
        'previous_inspection_date': previous_inspection_date

    #     # inspection sentiments (in progress)
    #     'sentiment_score':          round(sentiment_val, 4), 
    #     'sentiment_summary':        sentiment_summary_str,
    #     'main_inspection_topics':   key_inspection_themes_lst,

    #     'table_rows_found':len(df)
        }

# testing only 
def find_non_printable_characters(text):
    """
    TEST Finds and prints non-printable characters in the text.
    
    Args:
    text (str): The text to check for non-printable characters.
    
    Returns:
    None
    """
    non_printable = ''.join(ch for ch in text if ord(ch) < 32 or ord(ch) > 126)
    if non_printable:
        print(f"Non-printable characters found: {non_printable}")
    else:
        print("No non-printable characters found.")


def clean_pdf_content(pdf_content):
    # Check if pdf_content is bytes and decode to string
    if isinstance(pdf_content, bytes):
        pdf_content = pdf_content.decode('utf-8', errors='ignore')
    
    # Rem non-printing characters + non-text data
    text_content = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', pdf_content)
    
    # Rem remaining PDF encoding remnants and metadata
    text_content = re.sub(r'\\x[a-fA-F0-9]{2}', '', text_content)
    text_content = re.sub(r'[/<>\r\n]', ' ', text_content)  # Remove common non-text elements
    text_content = re.sub(r'\s{2,}', ' ', text_content)  # Replace multiple spaces with a single space
    
    # clean up the text
    text_content = text_content.strip()
    
    return text_content


def extract_text_from_pdf(pdf_bytes):
    # Open the PDF from bytes
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    extracted_text = ""
    
    # Iterate through each page
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        extracted_text += page.get_text("text")
    
    return extracted_text


def extract_text_by_pages(pdf_bytes):
    # supercedes extract_text_from_pdf in combo with remove_unwanted_sections
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []
    
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text = page.get_text("text")
        pages.append(text)
    
    return pages

def remove_unwanted_sections(pages_content):
     # supercedes extract_text_from_pdf in combo with extract_text_by_pages
     # we know the last two pages of the reports are superfluous to content/outcome detail
    cleaned_pages = []
    heading_found = False

    for page in pages_content:
        if "Local area partnership details" in page:
            heading_found = True
        
        if not heading_found:
            cleaned_pages.append(page)
    
    return cleaned_pages

def clean_text(text):
    # Replace newline characters that are directly joined with the following word with a space
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    # Remove extra newlines that don't separate paragraphs
    text = re.sub(r'\n\s*\n', '\n\n', text)
    # Replace double spaces with a single space
    text = re.sub(r' +', ' ', text)
    # Remove any trailing or leading whitespaces
    text = text.strip()

    text = text.replace('\n\n', ' ') # slightly frustrating brute force approach to persistent

    return text

def extract_inspection_outcome_section(cleaned_text):
    pattern = re.compile(r"Inspection outcome(.*?)Information about the local area partnership", re.DOTALL | re.IGNORECASE)
    match = pattern.search(cleaned_text)
    
    if match:
        section = match.group(1).strip()
        
        # Remove the last paragraph (assumes that more than 2 exist!)
        # This typically only states strategic progress publishing etc. 
        # E.g. "Ofsted and CQC ask that the local area partnership updates and publishes ...."
        paragraphs = re.split(r'\n\s*\n', section)
        
        if len(paragraphs) > 1:
            section = '\n\n'.join(paragraphs[:-1]).strip()
        else:
            section = section  # No change if there's only one paragraph

        section = clean_text(section)  # Clean further non-printing chars

        return section
    else:
        return "Inspection outcome section not found."



def determine_outcome_grade(inspection_outcome_section):
    grades = {
        "positive experiences": 1,
        "inconsistent experiences": 2,
        "significant concerns": 3
    }
    
    for phrase, grade in grades.items():
        if phrase in inspection_outcome_section:
            return grade
    
    return None  # If no matching phrase is found


def parse_inspection_date(date_string):

    formats = ["%d %B %Y", "%d/%m/%Y", "%d/%m/%y"]
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date format not supported {date_string} ")



def extract_next_inspection(inspection_outcome_section):
    monitoring_pattern = re.compile(r"monitoring inspection will be carried out within approximately (\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve) (years?|months?)", re.IGNORECASE)
    full_patterns = [
        re.compile(r"full reinspection will be within approximately (\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve) (years?|months?)", re.IGNORECASE),
        re.compile(r"the next full area SEND inspection will be within approximately (\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve) (years?|months?)", re.IGNORECASE)
    ]
    
    # Check for monitoring inspection first
    match = monitoring_pattern.search(inspection_outcome_section)
    if not match:
        # No intrim inspection was found, must be a full inspection due next
        for pattern in full_patterns:
            match = pattern.search(inspection_outcome_section)
            if match:
                break
    
    if match:
        # Convert text numbers to numeric
        number_map = {
            "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
            "eleven": 11, "twelve": 12
        }
        number_str = match.group(1).lower()
        time_frame = number_map.get(number_str, number_str)  # Convert text to number if needed
        unit = match.group(2).lower()
        
        return f"{time_frame} {unit}"
    
    return None  # If no matching time frame is found



def calculate_next_inspection_by_date(last_inspection_date, next_inspection_timeframe):
    if not last_inspection_date:
        return "Last inspection date not provided"

    if not next_inspection_timeframe:
        return "Next inspection time frame not found"

    # Parse the inspection_end_date
    try:
        last_inspection_date_parsed = parse_inspection_date(last_inspection_date)
    except ValueError as e:
        return str(e)

    # Extract number and unit from next_inspection_timeframe
    pattern = re.compile(r"(\d+) (years?|months?)", re.IGNORECASE)
    # print(type(next_inspection_timeframe))  # testing
    match = pattern.search(next_inspection_timeframe)
    
    if match:
        number = int(match.group(1))
        unit = match.group(2).lower()

        # testing
        print(f"calculate_next_inspection_by_date/number+unit: {number}, {unit}")  # testing

        if 'year' in unit:
            next_inspection_date = last_inspection_date_parsed + relativedelta(years=number)
        elif 'month' in unit:
            next_inspection_date = last_inspection_date_parsed + relativedelta(months=number)
        
        # testing
        #outgoing = next_inspection_date.strftime("%d/%m/%y")
        #print(f"calculate_next_inspection_by_date/next_date: {outgoing}")  # testing

        return next_inspection_date.strftime("%d/%m/%y")
    
    return "Invalid next inspection time frame"


def parse_date_new(date_input, date_format=None, output_format="%d/%m/%y", return_as_date=False):
    """
    Function to parse a date string or format a datetime object into a specified format, with an option to return as a date object.
    
    Args:
    date_input (str or datetime): The date string to be parsed or datetime object to be formatted.
    date_format (str, optional): A specific date format to be used for parsing. If not provided, multiple formats are tried.
    output_format (str, optional): The desired format for the output date string. Defaults to "%d/%m/%y".
    return_as_date (bool, optional): Whether to return the output as a datetime.date object. Defaults to False.
    
    Returns:
    str or datetime.date: The formatted date string in the specified output format, or a datetime.date object if return_as_date is True.
    
    Raises:
    ValueError: If the date string cannot be parsed with any of the supported formats.
    
    Notes:
    - Tries the provided date_format first if specified for parsing strings.
    - Falls back to trying a list of common formats if date_format is not provided or fails.
    - If the input is already a datetime object, formats it directly.
    """

    if date_input is None:
        return "" if not return_as_date else None
    
    # Check if the input is a datetime object
    if isinstance(date_input, datetime):
        date_obj = date_input
    else:
        # Check if the date_input is already in the desired output format
        try:
            date_obj = datetime.strptime(date_input, output_format)
            if return_as_date:
                return date_obj.date()
            else:
                return date_input  # Already in the desired output format
        except (ValueError, TypeError):
            pass  # Continue to parsing since it's not in the desired format
        
        # Try the provided date_format first if specified
        if date_format:
            try:
                date_obj = datetime.strptime(date_input, date_format)
            except (TypeError, ValueError):
                pass
    
        # Try the common formats
        formats = ["%d %B %Y", "%d/%m/%Y", "%d/%m/%y"]
        for fmt in formats:
            try:
                date_obj = datetime.strptime(date_input, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Date format for {date_input} is not supported")
    
    if return_as_date:
        

        return date_obj.date()
    else:

        return date_obj.strftime(output_format)
    


def process_provider_links(provider_links):
    """
    Processes provider links and returns a list of dictionaries containing URN, local authority, and inspection link.

    Args:
        provider_links (list): A list of BeautifulSoup Tag objects representing provider links.

    Returns:
        list: A list of dictionaries containing URN, local authority, inspection link, and, if enabled, additional inspection data.
    """
    
    data = []
    global pdf_data_capture # Bool flag
    global root_export_folder
    global inspections_subfolder


    for link in provider_links:
        # Extract the URN and provider name from the web link shown
        urn = link['href'].rsplit('/', 1)[-1]
        la_name_str = clean_provider_name(link.text.strip())


        provider_dir = os.path.join('.', root_export_folder, inspections_subfolder, urn + '_' + la_name_str)

        # Create the provider directory if it doesn't exist, ready for .pdf report export into file structure
        if not os.path.exists(provider_dir):
            os.makedirs(provider_dir)

        # Get the child page content
        child_url = 'https://reports.ofsted.gov.uk' + link['href']
        child_soup = get_soup(child_url)

        # Find all publication links in the provider's child page
        pdf_links = child_soup.find_all('a', {'class': 'publication-link'})


        # Initialise a flag to indicate if an inspection link has been found
        # Important: This assumes that the provider's reports are returned/organised most recent FIRST
        found_inspection_link = False

        # Iterate through the publication links
        for pdf_link in pdf_links:

            # E.g. Publication link contains
            # <a class="publication-link" href="https://files.ofsted.gov.uk/v1/file/50252240" target="_blank">


            # Check if the current/next href-link meets the selection criteria
            # This block obv relies on Ofsted continued use of nonvisual element descriptors
            # containing the type(s) of inspection text. We use  "children's services inspection"

            nonvisual_text = pdf_link.select_one('span.nonvisual').text.lower().strip()

            # For reference:
            # At this point <nonvisual_text> contains a mixed batch of the following:
            # joint area child protection inspection, pdf - 30 january 2024
            # children's services focused visit, pdf - 01 august 2024
            # joint area child protection inspection, pdf - 06 january 2023
            # children's services focused visit, pdf - 07 november 2023
            # area send full inspection, pdf - 12 july 2024

            # For now at least, web page|non-visual elements search terms hard-coded
            if 'area' in nonvisual_text and 'send' in nonvisual_text and 'full inspection' in nonvisual_text:

                # Create the filename and download the PDF (this filetype needs to be hard-coded here)
                filename = nonvisual_text.replace(', pdf', '') + '.pdf'


                # # For reference:
                # # at this point, example var contents would be: 
                # print(f"pdflink:{pdf_link}")                # e.g. "<a class="publication-link" href="https://files.ofsted.gov.uk/v1/file/50252437" target="_blank">
                #                                             # Area SEND full inspection                <span class="nonvisual">Area SEND full inspection, pdf - 15 July 2024</span></a>"
                # print(f"nonvisualtext:{nonvisual_text}")    # e.g. "area send full inspection, pdf - 15 july 2024"
                # print(f"filename:{filename}")               # e.g. "area send full inspection - 15 july 2024.pdf"
           


                # # Turn this OFF to minimise data 
                # # Download and stores locally each relevant PDF! 
                pdf_content = requests.get(pdf_link['href']).content
                # with open(os.path.join(provider_dir, filename), 'wb') as f:
                #     f.write(pdf_content)
                # ## END data reduction

  
                pdf_pages_content = extract_text_by_pages(pdf_content)
                pdf_pages_content_reduced = remove_unwanted_sections(pdf_pages_content)

                # Combine pages back into a single text
                pdf_content_reduced = "\n".join(pdf_pages_content_reduced)

                # Extract the "Inspection outcome" section
                inspection_outcome_section = extract_inspection_outcome_section(pdf_content_reduced)

                # Determine the outcome grade
                outcome_grade = determine_outcome_grade(inspection_outcome_section)
            
                # Next inspection time-frame (comnes back as f"{time_frame} {unit}")
                next_inspection = extract_next_inspection(inspection_outcome_section)

               # Extract the local authority and inspection link, and add the data to the list
                if not found_inspection_link:

                    # Capture the data that will be exported about the most recent inspection only
                    local_authority = provider_dir.split('_', 1)[-1].replace('_', ' ').strip()
                    inspection_link = pdf_link['href']
                    
                    # #testing
                    # print(f"la:{local_authority}")
                    # print(f"inspectionlink:{inspection_link}")

                

                    # Extract the report published date
                    report_published_date_str = filename.split('-')[-1].strip().split('.')[0] # published date appears after '-' 
            
                    # get/format date(s) (as dt objects)
                    report_published_date = format_date(report_published_date_str, '%d %B %Y', '%d/%m/%y')



                    # Now get the in-document data
                    if pdf_data_capture:
                        # Opt1 : ~x4 slower runtime
                        # Only here if we have set PDF text scrape flag to True
                        # Turn this off, speeds up script if we only need the inspection documents themselves to be retrieved

               
                        # Scrape inside the pdf inspection reports
                        # inspection_data_dict = extract_inspection_data(pdf_content)
                        inspection_data_dict = extract_inspection_data_update(pdf_content)
                    

                        # Dict extract here for readability of returned data/onward

                        # # inspection basics
                        # overall_effectiveness = inspection_data_dict['overall_inspection_grade']
                        # inspector_name = inspection_data_dict['inspector_name']
                        inspection_start_date = inspection_data_dict['inspection_start_date']
                        inspection_end_date = inspection_data_dict['inspection_end_date']
                        previous_inspection_date = inspection_data_dict['previous_inspection_date']


                        # format dates for output                       
                        inspection_start_date_formatted = format_date_for_report(inspection_start_date, "%d/%m/%y")
                        inspection_end_date_formatted = format_date_for_report(inspection_end_date, "%d/%m/%y")
                        previous_inspection_date_formatted = format_date_for_report(previous_inspection_date, "%d/%m/%Y") # Note YYYY not yy (required for placeholder date)

                        # Format the provider directory as a file path link (in readiness for such as Excel)
                        provider_dir_link = f"{provider_dir}"

                        
                        provider_dir_link = provider_dir_link.replace('/', '\\') # fix for Windows systems
                        
                        print(f"{local_authority}") # Gives listing console output during run in the format 'data/inspection reports/urn name_of_la'

                        # testing
                        #print(f"next_inspection: {next_inspection}")

                        # testing
                        #print(f"Dict: {inspection_data_dict}")
                        #print(f"inspection_start_date_formatted: {inspection_start_date}")
                        #print(f"inspection_end_date_formatted: {inspection_end_date}")
                        #print(f"inspection_start_date_formatted: {inspection_start_date_formatted}")
                        #print(f"inspection_end_date_formatted: {inspection_end_date_formatted} | next_inspection: {next_inspection}")

                        # problematic end date, means more likely to get success on start date (only 2/3 days difference)
                        next_inspection_by_date = calculate_next_inspection_by_date(inspection_start_date_formatted, next_inspection)

                        # testing
                        #print(f"next_inspection_by_date(after processing): {next_inspection_by_date}")

                        data.append({
                                        'urn': urn,
                                        'local_authority':          la_name_str,
                                        'inspection_link':          inspection_link,
                                        'outcome_grade':            outcome_grade,

                                        'previous_inspection_date': previous_inspection_date_formatted,
                                        'inspection_start_date':    inspection_start_date_formatted,
                                        'inspection_end_date':      inspection_end_date_formatted,
                                        'publication_date':         report_published_date,
                                        'next_inspection':          next_inspection,
                                        'next_inspection_by_date':  next_inspection_by_date,
                                        'local_link_to_all_inspections': provider_dir_link,
                                        'inspection_outcome_text':  inspection_outcome_section,

                                        # 'inspection_framework':   inspection_framework,
                                        # 'inspector_name':         inspector_name,

                                        # 'sentiment_score': sentiment_score,
                                        # 'sentiment_summary': sentiment_summary,
                                        # 'main_inspection_topics': main_inspection_topics

                                    })
                        
                    else:
                        # Opt2 : ~x4 faster runtime
                        # Only grab the data/docs we can get direct off the Ofsted page 
                        data.append({'urn': urn, 'local_authority': local_authority, 'inspection_link': inspection_link})

                    
                    found_inspection_link = True # Flag to ensure data reporting on only the most recent inspection
                

    return data


def save_data_update(data, filename, file_type='csv', hyperlink_column = None):
    """
    Exports data to a specified file type.

    Args:
        data (DataFrame): The data to be exported.
        filename (str): The desired name of the output file.
        file_type (str, optional): The desired file type. Defaults to 'csv'.
        hyperlink_column (str, optional): The column containing folder names for hyperlinks. Defaults to None.

    Returns:
        None
    """
    if file_type == 'csv':
        filename_with_extension = filename + '.csv'
        data.to_csv(filename_with_extension, index=False)

    elif file_type == 'excel':
        filename_with_extension = filename + '.xlsx'

        # Create a new workbook and add a worksheet
        workbook = xlsxwriter.Workbook(filename_with_extension)
        sheet = workbook.add_worksheet('ofsted_cs_send_inspections')  # pass the desired worksheet name here

        hyperlink_col_index = data.columns.get_loc(hyperlink_column) if hyperlink_column else None

        # Define hyperlink format
        hyperlink_format = workbook.add_format({'font_color': 'blue', 'underline': 1})

        # Write DataFrame to the worksheet
        for row_num, (index, row) in enumerate(data.iterrows(), start=1):
            for col_num, (column, cell_value) in enumerate(row.items()):
                if hyperlink_col_index is not None and col_num == hyperlink_col_index:
                    # Add hyperlink using the HYPERLINK formula
                    link = f".\\{cell_value}"
                    sheet.write_formula(row_num, col_num, f'=HYPERLINK("{link}", "{cell_value}")', hyperlink_format)
                else:
                    sheet.write(row_num, col_num, str(cell_value))

        # Write header
        header_format = workbook.add_format({'bold': True})
        for col_num, column in enumerate(data.columns):
            sheet.write(0, col_num, column, header_format)

        # Save the workbook
        workbook.close()
    else:
        print(f"Error: unsupported file type '{file_type}'. Please choose 'csv' or 'excel'.")
        return

    print(f"\n\n{filename_with_extension} successfully created!")



def import_csv_from_folder(folder_name):
    """
    Imports a single CSV file from a local folder relative to the root of the script.

    The CSV file must be located in the specified folder. If multiple CSV files are found,
    a ValueError is raised. If no CSV files are found, a ValueError is raised.

    Parameters:
    folder_name (str): The name of the folder containing the CSV file.

    Returns:
    pandas.DataFrame: A DataFrame containing the data from the CSV file.
    """
    file_names = [f for f in os.listdir(folder_name) if f.endswith('.csv')]
    if len(file_names) == 0:
        raise ValueError('No CSV file found in the specified folder')
    elif len(file_names) > 1:
        raise ValueError('More than one CSV file found in the specified folder')
    else:
        file_path = os.path.join(folder_name, file_names[0])
        df = pd.read_csv(file_path)
        return df
    
    
def reposition_columns(df, key_col, cols_to_move):
    """
    Move one or more columns in a DataFrame to be immediately to the right 
    of a given key column. 

    Args:
        df (pandas.DataFrame): The DataFrame to modify.
        key_col (str): The column that should be to the left of the moved columns.
        cols_to_move (list of str): The columns to move.

    Returns:
        pandas.DataFrame: The modified DataFrame.
    """
    # Check if the columns exist in the DataFrame
    for col in [key_col] + cols_to_move:
        if col not in df.columns:
            raise ValueError(f"{col} must exist in the DataFrame.")

    # Get a list of the column names
    cols = df.columns.tolist()

    # Find the position of the key column
    key_index = cols.index(key_col)

    # For each column to move (in reverse order)
    for col_to_move in reversed(cols_to_move):
        # Find the current index of the column to move
        col_index = cols.index(col_to_move)

        # Remove the column to move from its current position
        cols.pop(col_index)

        # Insert the column to move at the position immediately after the key column
        cols.insert(key_index + 1, col_to_move)

    # Return the DataFrame with reordered columns
    return df[cols]


def merge_and_select_columns(merge_to_df, merge_from_df, key_column, columns_to_add):
    """
    Merges two dataframes and returns a merged dataframe with additional columns from
    the second dataframe, without any duplicate columns. 

    Parameters:
    df1 (pandas.DataFrame): The first dataframe to merge.
    df2 (pandas.DataFrame): The second dataframe to merge.
    key_column (str): The name of the key column to merge on.
    columns_to_add (list): A list of column names from df2 to add to df1.

    Returns:
    pandas.DataFrame: A new dataframe with merged data from df1 and selected columns from df2.
    """
    merged = merge_to_df.merge(merge_from_df[columns_to_add + [key_column]], on=key_column)
    return merged




def save_to_html(data, column_order, local_link_column=None, web_link_column=None):
    """
    Exports data to an HTML table.

    Args:
        data (DataFrame): The data to be exported.
        column_order (list): List of columns in the desired order.
        hyperlink_column (str, optional): The column containing hyperlinks. Defaults to None.

    Returns:
        None
    """
    # Define the page title and introduction text
    page_title = "Ofsted CS SEND Inspections Overview"

    intro_text = f"""
    Summarised outcomes of published short and standard SEND inspection reports by Ofsted, refreshed weekly.<br/>

    An expanded version of the shown summary sheet, refreshed concurrently, is available to 
    <a href="{export_summary_filename}.xlsx">download here</a> as an .xlsx file. <br/>

    Data summary is based on the original <i>SEND Outcomes Summary</i> published periodically by the ADCS: 
    <a href="https://www.adcs.org.uk/inspection-of-childrens-services/">https://www.adcs.org.uk/inspection-of-childrens-services/</a>.
    """

    disclaimer_text = f"""
    Disclaimer: This summary is built from scraped data direct from 
    <a href="https://reports.ofsted.gov.uk/">https://reports.ofsted.gov.uk/</a> published PDF inspection report files.<br/><br/>

    Nuanced | variable inspection report content, structure, and PDF encoding occasionally result in problematic data extraction for a small number of LAs.<br/>

    <b>Known extraction issues:</b>
    <ul>
        <li><b>01/01/1900</b> == No-date-data | unreadable.</li>
        <li>Derbyshire, Enfield, North Yorkshire, Wiltshire (<b>Next Inspection By Date</b> not generated - but detail is available in summary text).</li>
        <li>Enfield, Wiltshire (<b>Next Inspection</b> timeframe not extracted - but detail is available in summary text).</li>
        <li>Derbyshire, North Yorkshire (<b>Previous, Start, End Inspection Date(s)</b> not extracted).</li>
    </ul>

    <a href="mailto:{d2i_contact_email}?subject=Ofsted-SEND-Scrape-Tool">Feedback</a> highlighting problems | inaccuracies | suggestions is welcomed.<br/>

    <a href="https://github.com/data-to-insight/ofsted-ilacs-scrape-tool/blob/main/README.md">
    Read the source ILACS tool/project for background details and future work.
    </a>.
    """

    data = data[column_order]
    global repo  # Use repo object initialised earlier



    # # Convert specified columns to title case
    # title_case_cols = ['local_authority', 'inspector_name']
    # for col in title_case_cols:
    #     if col in data.columns:
    #         data[col] = data[col].str.title()

    # # Temporary removal (#TESTING) for clarity | fixes
    # cols_to_drop = ['local_link_to_all_inspections', 'inspectors_inspections_count']
    # for col in cols_to_drop:
    #     if col in data.columns:
    #         data = data.drop(columns=col)


    # # If a local link column is specified, convert that column's values to HTML hyperlinks
    # # Displaying only the filename as the hyperlink text
    # if local_link_column:
    #     data[local_link_column] = data[local_link_column].apply(lambda x: '<a href="' + x + '">all_reports\\' + x.split("\\")[-1] + '</a>')


    # If a web link column is specified, convert that column's values to HTML hyperlinks
    # Shortening the hyperlink text by taking the part after the last '/'
    if web_link_column:
        data[web_link_column] = data[web_link_column].apply(lambda x: f'<a href="{x}">ofsted.gov.uk/{x.rsplit("/", 1)[-1]}</a>')

    # Convert column names to title/upper case
    data.columns = [c.replace('_', ' ').title() for c in data.columns]
    data.rename(columns={'Ltla23Cd': 'LTLA23CD', 'Urn': 'URN'}, inplace=True)


    # Generate 'Most-recent-reports' list (last updated list)
    # Remove this block if running locally (i.e. not in GitCodespace)
    # 
    # Obtain list of those inspection reports that have updates
    # Provides easier visual on new/most-recent on refreshed web summary page

    # specific folder to monitor for changes
    inspection_reports_folder = 'export_data/inspection_reports'


    ## moved/set elsewhere to enable git workflows - deleted after testing
    # try:
    #     # Init the repo object (so we know starting point for monitoring changes)
    #     repo = git.Repo(repo_path) 
    # except Exception as e:
    #     print(f"Error initialising defined repo path for inspection reports: {e}")
    #     raise
    
    
    try:
    # Get current status of repo
        changed_files = [item.a_path for item in repo.index.diff(None) if item.a_path.startswith(inspection_reports_folder)]
        untracked_files = [item for item in repo.untracked_files if item.startswith(inspection_reports_folder)]

        # Combine tracked and untracked changes
        all_changed_files = changed_files + untracked_files

        # Remove the inspection_reports_folder path prefix from the file paths
        las_with_new_inspection_list = [os.path.relpath(file, inspection_reports_folder) for file in all_changed_files]

        # Remove "/children's services inspection" and ".pdf" from each list item string
        # overwrite with cleaned list items. 
        las_with_new_inspection_list = [re.sub(r"/children's services inspection|\.pdf$", "", file) for file in las_with_new_inspection_list]

        # # Verification output only
        # print("Changed files:", changed_files)
        # print("Untracked files:", untracked_files)
        # print("All changed files:", all_changed_files)
        print("Last updated list:", las_with_new_inspection_list)

    except Exception as e:
        print(f"Error processing repository: {e}")
        raise

# end of most-recent-reports generate
# Note: IF running this script locally, not in Git|Codespaces - Need to chk + remove any onward use of var: las_with_new_inspection_list 

    

    # current time, add one hour to the current time to correct non-UK Git server time
    adjusted_timestamp_str = (datetime.now() + timedelta(hours=1)).strftime("%d %m %Y %H:%M")

    # init HTML content with title and CSS
    html_content = f"""
    <html>
    <head>
        <title>{page_title}</title>
        <style>
            .container {{
                display: flex;
                justify-content: center;
                align-items: center;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 10pt;
            }}
            table, th, td {{
                border: 1px solid #ddd;
            }}
            th, td {{
                padding: 5px;
                text-align: left;
            }}
        </style>
    </head>
    <body>
        <h1>{page_title}</h1>
        <p>{intro_text}</p>
        <p>{disclaimer_text}</p>
        <p><b>Summary data last updated: {adjusted_timestamp_str}</b></p>
        <p><b>LA inspections last updated: {las_with_new_inspection_list}</b></p>
        <div class="container">
    """

    # Convert DataFrame to HTML table
    html_content += data.to_html(escape=False, index=False)

    # Close div and HTML tags
    html_content += "\n</div>\n</body>\n</html>"

    # Write to index.html
    with open("index.html", "w") as f:
        f.write(html_content)

    print("SEND summary page as index.html successfully created!")






#
# Scrape Ofsted inspection report data
#

data = []
while start < max_results:
    # Construct URL for current chunk
    url = url_stem + search_url + pagination_param.format(start=start)

    print(f"Fetching: {url}")  # Debug output

    # Fetch and parse search page
    soup = get_soup(url)

    if soup is None:
        print("⚠️ ERROR: No content retrieved, stopping.")
        break

    # Find provider links
    provider_links = soup.find_all('a', href=lambda href: href and '/provider/' in href)

    print(f"🔍 DEBUG: Found {len(provider_links)} provider links on page {start}-{start + max_page_results}")

    if not provider_links:
        break  # no more results found

    # provider links
    data.extend(process_provider_links(provider_links))

    # continue on next batch (if there is)
    start += max_page_results


# Convert the 'data' list to a DataFrame
send_inspection_summary_df = pd.DataFrame(data)

# # testing
# print(send_inspection_summary_df.head(5))

# Data enrichment - import flat-file stored data 
#

# Enables broader potential onward usage/cross/backwards-compatible access 
# Note: Where possible, avoid any reliance on flat-file stored dynamic data! 
#       - This process idealy only for static data, or where obtaining specific data points in a dynamic manner isnt possble etc. 
#       - These just examples of potential enrichment use-cases




# Enrichment1: LA codes
# Ofsted data centres on URN, but some might need historic 'LA Number'

# import the needed external/local data
local_authorities_lookup_df = import_csv_from_folder(import_la_data_path) # bring external data in

# print(local_authorities_lookup_df.head(3))
# print(send_inspection_summary_df.head(3)) # empty


# Ensure key column consistency
key_col = 'urn'
send_inspection_summary_df['urn'] = send_inspection_summary_df['urn'].astype('int64')
local_authorities_lookup_df['urn'] = pd.to_numeric(local_authorities_lookup_df['urn'], errors='coerce')

# # Define what data is required to be merged in
additional_data_cols = ['la_code', 'region_code', 'ltla23cd', 'stat_neighbours']
send_inspection_summary_df = merge_and_select_columns(send_inspection_summary_df, local_authorities_lookup_df, key_col, additional_data_cols)

# re-organise column structure now with new col(s)
send_inspection_summary_df = reposition_columns(send_inspection_summary_df, key_col, additional_data_cols)
## End enrichment 1 ##




# #
# # Fix(tmp) towards resultant export data types/excel cols type or format

# # 020523 - Appears as though this is not having the desired effect once export file opened in Excel.
# # Needs looking at again i.e. Urn still exporting as 'text' column

# ilacs_inspection_summary_df['urn'] = pd.to_numeric(ilacs_inspection_summary_df['urn'], errors='coerce')
# ilacs_inspection_summary_df['la_code'] = pd.to_numeric(ilacs_inspection_summary_df['la_code'], errors='coerce')
# # end tmp fix






# Export summary data (visible outputs)
#

# EXCEL Output
# Also define the active hyperlink col if exporting to Excel
save_data_update(send_inspection_summary_df, export_summary_filename, file_type=export_file_type, hyperlink_column='local_link_to_all_inspections')


# WEB Output
# Set up which cols to take forward onto the web front-end(and order of)
# Remove for now until link fixed applied: 'local_link_to_all_inspections',
column_order = [
                'urn','la_code','region_code','ltla23cd','local_authority',
                'previous_inspection_date',
                'inspection_start_date', 'inspection_end_date',
                'outcome_grade', 
                'inspection_outcome_text',
                'publication_date', 'next_inspection', 'next_inspection_by_date',
                #'local_link_to_all_inspections', 
                'inspection_link'
                ]


save_to_html(send_inspection_summary_df, column_order, local_link_column='local_link_to_all_inspections', web_link_column='inspection_link')


print("Last output date and time: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


