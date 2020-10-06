#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:35:12 2020

@author: RobertTeresi
"""

from pathlib import Path  # Detect home path
from selenium import webdriver  # Webdriver base class
import pandas as pd
from pushbullet import Pushbullet  # Push notifications to the scraper
import time
from selenium.webdriver.common.keys import Keys  # To send common keys
import re  # Regex
import functools  # For custom error handling metaclass
import pickle
from datetime import datetime
from datetime import date
import threading  # To format staff schedules in another thread

emed_shifts = ['YNHH-YSC A(Purple)1 0700-1500', '	YNHH-YSC B1 0700-1500',
               'SMC Sat 1  0700-1500', 'YNHH-YSC A(Green)1 0900-1700',
               '	YNHH YSC C1 1100-1900', 'YNHH-YSC A(Purple)2 1500-2300',
               'YNHH YSC B2 1500-2300', 'SMC Sat 2  1500-2300',
               'YNHH-YSC A(Green)2 1700-0100', 'YNHH-YSC C2 1700-0100',
               'YNHH-YSC A3 2300-0700', 'YNHH-YSC B3 2300-0700',
               'SMC Sat 3  2300-0700', 'YNHH-SRC AD 0700-1500',
               'YNHH-SRC BD 0600-1400', 'YNHH-SRC CD 900-1700',
               'YNHH-SRC AE 1500-2300', 'YNHH-SRC BE 1400-2200',
               'YNHH-SRC CE 1700-0100', 'YNHH-SRC AN 2300-0700',
               'YNHH-SRC BN 2200-0600'
               'SMC Sat 1 0700-1500', 'SMC Sat 2 1500-2300',
               '	SMC Sat 3 2300-0700', '    YSC Critical Care 1 0700-1700',
               'YSC Critical Care 2 1700-0200', 'YNHH-YSC Float 1 0900-1700']


# Define out error catching metaclass #
def catch_exception(f):
    """Define error output behavior metaclass."""
    @functools.wraps(f)
    def func(*args, **kwargs):
        # It tries to run the function
        try:
            return f(*args, **kwargs)

        # And, if there's an error...
        except StopIteration:
            return f(*args, **kwargs)
        except Exception:

            # It gives us the name of the function that we caught the error in
            print('\n ERROR! \n Caught an exception in the ', f.__name__,
                  " function \n\n")

            # And the full traceback
            raise
    return func


class ErrorCatcher(type):
    """Tells derived classes to output type of function error message."""

    def __new__(cls, name, bases, dct):
        """Wrap callable functions with catch_exception function."""
        for m in dct:
            if hasattr(dct[m], '__call__'):
                dct[m] = catch_exception(dct[m])
        return type.__new__(cls, name, bases, dct)


class shift_scraper(webdriver.Firefox, metaclass=ErrorCatcher):
    """When complete, will be the class that parses shifts and outputs a DF."""

    # Initialize the class. Import data and set relevant personal computer info
    def __init__(self):
        driver_path = r'/anaconda3/lib/python3.7/geckodriver'
        api_key = 'o.ejFbHaDwYysBB2NbpBcQodzagxEFGVTH'
        self.pb = Pushbullet(api_key)

        super(shift_scraper, self).__init__(executable_path=driver_path)

        # Set the implicit wait time to 8 seconds
        self.implicitly_wait(8)
        self.wd = str(Path.home()) + "/Dropbox/CoViD_ED_TF/"
        self.resident_phones = pd.read_csv(self.wd + "Resident_Phones.csv")


def get_doctors():
    """Parse ED Physician Shifts."""
    global shift_df
    # Make a dictionary to append to a dataframe with doctor info
    shiftinfo = dict.fromkeys(['role', 'area', 'time',
                               'name', 'phone'])

    # Go to amion
    scraper.get('http://www.amion.com')

    time.sleep(1)
    # Type in password
    scraper.find_element_by_css_selector("[name='Login']").send_keys('password')
    time.sleep(1)
    # And enter
    scraper.find_element_by_css_selector("[name='Login']").send_keys(Keys.RETURN)
    time.sleep(1)
    # Click on Emergency Medecine
    # A terrible way for finding the right link
    links = scraper.find_elements_by_css_selector('a')

    i = 0
    for link in links:
        if re.search(r"^Emergency\s*Medicine",
                     link.get_attribute('innerText')):
            print(link.get_attribute('innerText'))
            break
        i += 1

    clicked = False
    while not clicked:
        try:
            links[i].click()
            clicked = True
        except Exception:
            pass

    # Contains all info - we can find what we need by following some rules
    shiftdatapoints = scraper.find_elements_by_css_selector('nobr')

    # intialize some helper variables
    last_was_time, unnecessary_area, last_was_phone, hold_area = (False, False,
                                                                  False, [])

    # We are going to loop through all of the datapoints to get what we need
    for datum in shiftdatapoints:
        # There are a few second contacts that we don't want to mess us up
        if(re.search(r'beeper|After hours',
                     datum.get_attribute('innerText'))):
            last_was_phone = True
            continue

        # If the last thing we got was the
        if last_was_time:
            shiftinfo['name'] = datum.get_attribute('innerText')
            last_was_time = False

        # If the following regex returns true then the cell is a shift
        if(re.search(r'\d{4}\-\d{4}', datum.get_attribute('innerText'))):
            last_was_phone = False
            # We only want to get info for shifts we care about (in a list)
            if re.search(r'^(YNHH|YSC|SMC)\-?(YNHH|YSC|SMC)? '
                         r'(?!Back\-Up)[\-a-zA-Z ]+\d? \d{4}\-\d{4}$',
                         datum.get_attribute('innerText')):
                unnecessary_area = False

                # This signals the start of a new doctor-shift

                # Check the dictionary isn't empty (first iteration)
                shiftinfo = dict.fromkeys(['role', 'area', 'name', 'phone'])

                # Add the role and shift
                shiftinfo['role'] = 'physician'
                shiftinfo['area'] = re.sub(r" \d{4}\-\d{4}", "",
                                           datum.get_attribute('innerText'))
            else:
                # Set a variable that will tell us not to collect info
                unnecessary_area = True
        if not unnecessary_area:
            # Here it it matches a time
            if(re.search(r'\d{1,2}[ap]\-\d{1,2}[ap]',
                         datum.get_attribute('innerText'))):
                # If last entry was a phone number
                # this is the beginning of a new shift in the same area
                if last_was_phone:
                    shiftinfo['area'] = hold_area
                    shiftinfo['role'] = 'physician'

                shiftinfo['time'] = datum.get_attribute('innerText')
                last_was_time = True
                last_was_phone = False

            # And Now Phone Numbers
            if(re.search(r'\d{3}\-\d{3}\-\d{4}',
                         datum.get_attribute('innerText'))):
                # We want the phone number to be the second if possible
                if last_was_phone:
                    shift_df['phone'].iloc[-1] = (datum.
                                                  get_attribute('innerText'))
                else:
                    shiftinfo['phone'] = datum.get_attribute('innerText')
                    shift_df = shift_df.append(shiftinfo, ignore_index=True)
                    hold_area = shiftinfo['area']
                    shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                               'name', 'phone'])
                last_was_phone = True
    return(shift_df)


def get_pa_amion():
    """Parse the shifts for the PAs."""
    global shift_df
    # Make a dictionary to append to a dataframe with doctor info
    shiftinfo = dict.fromkeys(['role', 'area', 'time',
                               'name', 'phone'])

    # Go to amion
    scraper.get('http://www.amion.com')

    # Type in password
    scraper.find_element_by_css_selector("[name='Login']").send_keys('password')

    # And enter
    scraper.find_element_by_css_selector("[name='Login']").send_keys(Keys.RETURN)

    time.sleep(1)
    # Click on Emergency Medecine
    # A terrible way for finding the right link
    links = scraper.find_elements_by_css_selector('a')

    i = 0
    for link in links:
        if re.search(r"^Emergency\s*Department",
                     link.get_attribute('innerText')):
            print(link.get_attribute('innerText'))
            break
        i += 1

    clicked = False
    while not clicked:
        try:
            links[i].click()
            clicked = True
        except Exception:
            time.sleep(1)

    # Contains all info - we can find what we need by following some rules
    shiftdatapoints = scraper.find_elements_by_css_selector('.grbg td')

    # intialize some helper variables
    (last_was_time,
     unnecessary_area,
     last_was_phone,
     hold_area) = False, False, False, []
    for datum in shiftdatapoints:
        # There are a few second contacts that we don't want to mess us up
        if(re.search(r'beeper|After hours', datum.get_attribute('innerText'))):
            last_was_phone = True
            continue

        # If the last thing we got was time
        if last_was_time:
            shiftinfo['name'] = datum.get_attribute('innerText')
            last_was_time = False

        # If the following regex returns true then the cell is a shift
        if(re.search(r"^['A-Z]+[A-Za-z\0-9\/\s]+\d{1,2}[ap]\-\d{1,2}[ap]",
                     datum.get_attribute('innerText'))):
            last_was_phone = False
            # We only want to get info for shifts we care about (in a list)
            if 'Backup' not in datum.get_attribute('innerText'):
                unnecessary_area = False

                # This signals the start of a new doctor-shift

                # Check the dictionary isn't empty (first iteration)
                shiftinfo = dict.fromkeys(['role', 'area',
                                           'name', 'phone'])

                # Add the role and shift
                shiftinfo['role'] = 'PA'
                shiftinfo['area'] = re.sub(r'\d{1,2}[ap]\-\d{1,2}[ap]', "",
                                           datum.get_attribute('innerText'))
            else:
                unnecessary_area = True
                # Otherwise set variable that will tell us not to collect info

        if not unnecessary_area:
            # Here it it matches a time
            if(re.search(r'^\d{1,2}[ap]\-\d{1,2}[ap]',
                         datum.get_attribute('innerText'))):
                # If the last entry was a phone number,
                # then this is the beginning of a new shift in the same area.
                if last_was_phone:
                    shift_df = shift_df.append(shiftinfo, ignore_index=True)
                    hold_area = shiftinfo['area']
                    shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                               'name', 'phone'])
                    shiftinfo['area'] = hold_area
                    shiftinfo['role'] = 'PA'
                    del hold_area

                shiftinfo['time'] = datum.get_attribute('innerText')
                last_was_time = True
                last_was_phone = False
            # And Now Phone Numbers
            if(re.search(r'\d{3}\-\d{3}\-\d{4}',
                         datum.get_attribute('innerText'))):
                if last_was_phone:
                    pass
                else:
                    shiftinfo['phone'] = datum.get_attribute('innerText')
                    shift_df = shift_df.append(shiftinfo, ignore_index=True)
                    shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                               'name', 'phone'])
                last_was_phone = True
    return(shift_df)


def yaleem_residents():
    """Parse shifts for the ED Residents."""
    global shift_df
    scraper.get("https://www.yaleem.org/")
    time.sleep(1)
    scraper.find_element_by_css_selector("a[href='/schedule']").click()
    time.sleep(2)
    scraper.find_element_by_css_selector("[name='password']").send_keys("password")
    time.sleep(0.5)
    scraper.find_element_by_css_selector("[name='password']").send_keys(Keys.RETURN)
    time.sleep(1)
    scraper.find_element_by_css_selector(".sqs-block-button-element").click()
    time.sleep(4)
    # Make sure we are on the correct page
    scraper.find_elements_by_css_selector("tr [value='Go to Today']")[0].click()
    time.sleep(2)

    # Every even is the shift, every odd is the Resident
    todays_shifts = scraper.find_elements_by_css_selector('td.calendar-today div span')

    # Weirdly this changes on the weekend?
    if not todays_shifts:
        todays_shifts = scraper.find_elements_by_css_selector('.calendar-weekend-today div span')

    # If still not today's shifts:
    if not todays_shifts:
        print("Problem retrieving data from yaleem")

    shiftinfo = dict.fromkeys(['role', 'area', 'time', 'name', 'phone'])
    for datum in todays_shifts:
        if re.search(r'\d{4}\-\d{4}', datum.get_attribute('innerText')):
            shiftinfo['role'] = 'Resident'
            shiftinfo['area'] = datum.get_attribute('innerText')
            shiftinfo['time'] = datum.get_attribute('innerText')[-9:]
        else:
            shiftinfo['name'] = datum.get_attribute('innerText')
            try:
                shiftinfo['phone'] = scraper.resident_phones.loc[scraper.resident_phones['Last Name'] == shiftinfo['name'],"Personal Phone"].iloc[0]
                shift_df = shift_df.append(shiftinfo, ignore_index=True)
                shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                           'name', 'phone'])
            except Exception:
                shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                           'name', 'phone'])


# This threaded function will  be run alongside the webscraper.
def staff_parse():
    """Parse the ED staff shifts into the shift dataframe."""
    global staff_df
    staff_shift_df = pd.read_csv(str(Path.home()) + "/Dropbox/CoViD_ED_TF/" +
                                 "ED_schedule_Active.csv")
    staff_phones = pd.read_csv(str(Path.home()) + "/Dropbox/CoViD_ED_TF/" +
                               "ED_staff_phone_numbers.csv")

    datetime.strptime(staff_shift_df.columns[1:][0], '%m/%d/%y')

    # Return the schedule with only today's shifts
    for col in staff_shift_df.columns[1:]:
        if datetime.combine(date.today(), datetime.min.time()) == datetime.strptime(col,'%m/%d/%y'):
            print(col)
            todays_staff_shifts = staff_shift_df[['Employee', col]]
            del staff_shift_df
            break

    staff_shift_df = pd.DataFrame(columns=['role', 'area', 'time',
                                           'name', 'phone'])
    # Get the people are working, their shift, and their phone numbers
    for index, row in todays_staff_shifts.iterrows():
        # Make dictionary
        staffshift = dict.fromkeys(['role', 'area', 'time', 'name', 'phone'])
        staffshift['role'], staffshift['area'], staffshift['name'] = (
            "Tech/Nurse", None, row['Employee'])

        # Find shifts. They are chronological so take the last one
        try:
            staffshift['time'] = re.findall("\d{1,2}:\d\d[AP]M\s{0,3}\-\s{0,3}\d{1,2}:\d\d[AP]M", row[1], re.DOTALL)[-1]
        except IndexError:  # Finding no matches will give this error
            pass
        except TypeError:  # Inputting None type will give this error
            pass
        except Exception:
            raise
        if staffshift['time']:
            try:
                staffshift['phone'] = staff_phones.loc[staff_phones['Name'] == staffshift['name'],"Home Phone"].iloc[0]
            except Exception:
                pass
            if pd.isna(staffshift['phone']):
                continue
            staff_shift_df = staff_shift_df.append(staffshift,
                                                   ignore_index=True)
    staff_df = staff_shift_df


def aed_parse():
    """
    Parse shifts for AED(?) staff.

    Returns
    -------
    Appends shifts to global staff_df.

    """
    global staff_df
    data = pd.read_csv(str(Path.home()) + '/Dropbox/CoViD_ED_TF/AED.csv')

    # N days since start mod 14 will tell us correct column indefinitely
    colnum = (datetime.now() -
              datetime(2020, 5, 10, 0)).days % 14 + 3  # May 10th is day 1

    daycolumn = data.columns[colnum]
    columns = ['Employee Name', 'Section', 'Phone', daycolumn]

    data = data[columns]

    for index, row in data.iterrows():
        if re.search(r'\d', row[daycolumn]):
            shift_dict = {'role': 'AED',
                          'area': row['Section'],
                          'time': row[daycolumn],
                          'name': row['Employee Name'],
                          'phone': row['Phone']}
            staff_df = staff_df.append(shift_dict,
                                       ignore_index=True)


def get_pharmacists():
    """Add ED pharmacists to shift dataframe."""
    global staff_df
    phones = dict({
        "Hultz, Kyle": "610-763-3165",
        "Zahn, Evan": "765-432-9479",
        "Albano, Jesse": "949-735-1714"
        })
    # Bring in the data
    pharmacists = pd.read_csv(str(Path.home()) +
                              "/Dropbox/CoViD_ED_TF/" +
                              "Pharmacy_Schedule_Active.csv")
    pharmacists = pharmacists.fillna("")
    for col in pharmacists.columns[1:]:
        if (datetime.combine(date.today(), datetime.min.time()) ==
                datetime.strptime(col, '%m/%d/%Y')):
            print(col)
            pharmacists = pharmacists[['Employee', col]]
            break
    for index, row in pharmacists.iterrows():
        if re.search(r'\d\d\-\d', row[col]):
            shiftinfo = dict.fromkeys(['role', 'area', 'time',
                                       'name', 'phone'])
            shiftinfo['role'] = 'pharmacist'
            shiftinfo['area'] = 'ED Pharmacy'
            shiftinfo['time'] = re.search(r'\d{2}\-\d{1,2}', row[col]).group(0)
            shiftinfo['name'] = row['Employee']
            shiftinfo['phone'] = phones[shiftinfo['name']]
            staff_df = staff_df.append(shiftinfo, ignore_index=True)


def threaded_fun():
    """Run a few  functions in a second thread while main thread scrapes."""
    staff_parse()
    aed_parse()
    get_pharmacists()


# Start thread that formats the nurses'/techs'  shifts
staff_df = [None]
thread1 = threading.Thread(target=threaded_fun)
thread1.start()

# Initialize scraper
scraper = shift_scraper()

# This will compile a new shift chart for each day

shift_df = pd.DataFrame(columns=['role', 'area', 'time',
                                 'name', 'phone'])

# So far we are getting doctors, APPs, and residents
# Get Doctors
get_doctors()

# Get APPs
get_pa_amion()

# Get Residents
yaleem_residents()

# Make sure the other thread has finished
thread1.join()  # Returns our global staff_df variable

# Now we append the two together
shift_df = shift_df.append(staff_df, ignore_index=True)

# Some physicians have requested to use alternate phones below
shift_dic = {"475-224-7912": "720-236-8994",
             "475-224-7924": "203-676-3238",
             "475-334-8253": "203-688-4275"}

# Remove shifts that are blank !
shift_df = shift_df.dropna(subset=['role', 'phone'])

# Replace primary phones with secondary phones if they are in the dictionary
shift_df['phone'] = shift_df['phone'].apply(lambda x: shift_dic[x.strip()]
                                            if x.strip() in shift_dic else x)

# Then we want to export shifts
shift_df.to_csv(scraper.wd + "shift_df.csv")

# Pass along a variable to the next script that will stop execution
# at the beginning of the next day if set to True
stop_script = False
with open((scraper.wd + "stop_script.pkl"), 'wb') as f:
    pickle.dump(stop_script, f)

# Quit our scraper to clean up nicely
scraper.quit()
