#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:40:16 2020

@author: RobertTeresi
"""

import time  # Sleep
from datetime import datetime  # Time format
from datetime import timedelta  # Add and subtract time
from datetime import date  # Gets Date
import operator  # To sort tuples
import re  # Regex
from pathlib import Path  # Home directory shortcut
import pandas as pd  # Data
from twilio.rest import Client  # API
from twilio.base.exceptions import TwilioRestException
import threading  # To save
import subprocess
import pickle  # Store  stop variable
import pause

# FILE PARAMETERS #
# Set need to update to False if the shift_df is up to date, True otherwise
need_to_update = True

# "Normal Mode" Sends  texts 15 mins after end of shift
normal_mode = True  # False sends texts  according to ad hoc scheme

# Debug mode doesn't send texts and doesn't save data
debug_mode = True  # Set to False to run, True to Debug
stop_script = False   # Set to True in shift_scraper to stop on next update

# Delay start
delayed_start = False
start_time = datetime(2020, 5, 28, 6, 0)  # Year, month, day, hour, minute

# Number of shifts between texts
n_shifts_between_texts = 10

# Twilio account info - sets up API
account_sid = "ENTER SID HERE"
auth_token = "ENTER AUTH TOKEN HERE"
client = Client(account_sid, auth_token)


# FUNCTIONS #

def update_data():
    """Upload changes in data tracking shifts since last text."""
    ts_path = str(Path.home())+"/Dropbox/CoViD_ED_TF/text_scheduler_df.csv"
    text_scheduler_df.to_csv(ts_path)
    DNClist.to_csv(str(Path.home())+"/Dropbox/CoViD_ED_TF/DNClist.csv")
    textlog.to_csv(str(Path.home())+"/Dropbox/CoViD_ED_TF/textlog.csv")


def sort_jobs(already_called=False):
    """Sorts shifts into chronological order by time shift ends."""
    global listofjobs
    # First we want to aviod double-counting double-shifts
    # To do so sort the tuple by phone then time
    try:
        listofjobs.sort(key=operator.itemgetter(0, 1))

        # Now we can loop through the jobs
        previous_phone = None
        index = 0
        for tple in listofjobs:
            if previous_phone == tple[0]:
                listofjobs.pop(index)
            previous_phone = tple[0]
            index += 1
        try:
            listofjobs.sort(key=operator.itemgetter(1))
        except TypeError:
            print("Likely None Type in list of jobs: trying again")
            if not already_called:
                index = 0
                for tple in listofjobs:
                    if tple[1] is None:
                        listofjobs.pop(index)
                        print("None type removed. " + tple[0] + "\n")
                    index += 1
                sort_jobs(already_called=True)
            else:
                raise
        return(print("jobs sorted"))
    except Exception:
        print("Error while sorting jobs")
        raise


def scrape_shifts():
    """Call script that scrapes shift data from internet."""
    subprocess.call(["python",
                     str(Path.home())+"/Dropbox/CoViD_ED_TF/"
                     "shift_scraper.py"])


# Following functions get time and phone into a consistent format #
def phone_format(phone_number):
    """Format phone numbers."""
    phone_number = phone_number.strip()
    return("+1" + re.sub(r'\-|\(|\)|\.', '', phone_number))


# Format times
def time_format(time, normal_mode=True):
    """Massage time formats into one consistent form."""
    try:

        # JUST FOR RESIDENTS SO FAR
        # Military time no colons
        time = time.strip()
        if re.search(r'\d{4}\-\d{4}', time):
            endshift = re.search(r'(\d{4})\-(\d{4})', time).group(2)
            hour = int(endshift[:2])
            minute = int(endshift[2:])

            # If we are doing weird timings
            if not normal_mode:
                beginshift = endshift = re.search(r'(\d{4})\-(\d{4})',
                                                  time).group(1)
                hour = int(beginshift[:2])
                minute = int(beginshift[2:])

                # Is this a morning shift, night shift, or neither? (UGH)
                if hour < 8:  # Morningshift
                    return(format_datetime(date.today().month,
                                           date.today().day, 8, 15))
                # Nightshift
                elif (19 < hour) & (22 > hour):
                    return(format_datetime(date.today().month,
                                           date.today().day, 22, 15))

                else:  # Send a text 4 hours after shift starts
                    return(datetime(2020, date.today().month, date.today().day,
                                    hour, minute, 0, 0) + timedelta(hours=4))

            # If the shift is overnight we change the day to tomorrow
            if (int(re.search(r'(\d{4})\-(\d{4})', time).group(1)) >=
                    int(re.search(r'(\d{4})\-(\d{4})', time).group(2))):
                return(format_datetime((date.today()+timedelta(days=1)).month,
                                       (date.today()+timedelta(days=1)).day,
                                       hour, minute))
            else:
                return(format_datetime(date.today().month,
                                       date.today().day, hour, minute))
        # For 7a-7p we can convert to military and call this function again
        elif re.search(r'^\d{1,2}[ap]\-\d{1,2}[ap]', time):
            beginshift = re.search(r'^(\d{1,2}[ap])\-(\d{1,2}[ap])',
                                   time).group(1)
            endshift = re.search(r'^(\d{1,2}[ap])\-(\d{1,2}[ap])',
                                 time).group(2)

            # Super hacky
            if beginshift[-1] == "p":
                beginshift = str(int(beginshift[:-1]) + 12) + "00"
            elif int(beginshift[:-1]) < 10:
                beginshift = "0" + beginshift[:-1] + "00"
            else:
                beginshift = beginshift[:-1] + "00"
            if endshift[-1] == "p":
                endshift = str(int(endshift[:-1]) + 12) + "00"
            elif int(endshift[:-1]) < 10:
                endshift = "0" + endshift[:-1] + "00"
            else:
                endshift = endshift[:-1] + "00"

            if beginshift == "2400":
                beginshift = "1200"
            elif beginshift == "1200":
                beginshift = "0000"
            elif endshift == "2400":
                endshift = "1200"
            elif endshift == "1200":
                endshift = "0000"
            else:
                pass
            return(time_format((beginshift + "-" + endshift), normal_mode))

        # Nurse format - convert to Resident format
        elif re.search(r'\d{1,2}:\d\d[[AaPp][Mm]?\s{0,3}\-\s{0,3}\d{1,2}:\d\d[[AaPp][Mm]?',
                       time):
            print("nurse")
            time = re.sub(r":|\s", "", time).upper()

            beginshift = re.search(r'(\d{3,4}[AP][M]?)\s{0,3}\-\s{0,3}(\d{3,4}[AP][M]?)',
                                   time).group(1)
            endshift = re.search(r'(\d{3,4}[AP][M]?)\s{0,3}\-\s{0,3}(\d{3,4}[AP][M]?)',
                                 time).group(2)

            if beginshift[-2:] == "PM":
                beginshift = str(int(beginshift[:-4]) + 12) + beginshift[-4:-2]
            elif int(beginshift[:-4]) < 10:
                beginshift = "0" + beginshift[:-2]
            else:
                beginshift = beginshift[:-2]

            if endshift[-2:] == "PM":
                endshift = str(int(endshift[:-4]) + 12) + endshift[-4:-2]
            elif int(endshift[:-4]) < 10:
                endshift = "0" + endshift[:-2]
            else:
                endshift = endshift[:-2]

            if beginshift == "2400":
                beginshift = "1200"
            elif beginshift == "1200":
                beginshift = "0000"
            elif endshift == "2400":
                endshift = "1200"
            elif endshift == "1200":
                endshift = "0000"
            else:
                pass

            return(time_format((beginshift + "-" + endshift), normal_mode))

        # Pharmacists
        elif re.search(r'\d{2}\-\d{1,2}', time):
            start = int(re.search(r'(\d{2})\-(\d{1,2})', time).group(1))
            duration = int(re.search(r'(\d{2})\-(\d{1,2})', time).group(2))
            endshift = datetime(2020, date.today().month, date.today().day,
                                start, 0, 0) + timedelta(hours=duration)
            if not normal_mode:
                if start < 8:  # Morningshift
                    return(format_datetime(date.today().month,
                                           date.today().day, 8, 15))

                elif (19 < start) & (22 > start):  # Nightshift
                    return(format_datetime(date.today().month,
                                           date.today().day, 22, 15))

                else:  # Send a text 4 hours after shift starts
                    return(datetime(2020, date.today().month, date.today().day,
                                    start, 0, 0, 0) + timedelta(hours=4))
            return(format_datetime(endshift.month, endshift.day,
                                   endshift.hour, endshift.minute))

        else:
            print("Not a recognized time format")
            raise Exception
    except Exception:
        print(time)
        raise


def format_datetime(month, day, hour, minute):
    """Small Helper Function to format and return datetime."""
    return(datetime(2020, month, day, hour, minute, 0, 0) +
           timedelta(minutes=-15))


def name_format(name, role):
    """Find appropriate name for each staff member."""
    # First try to get the first name name
    # Splitting on the comma, we should get two segments for last, first
    if len(re.split(r', ?', name)) == 2:
        # Now we have to deal with initials
        if len(re.split(r', ?', name)[1]) < 2:
            if role == "physician" or role == "Resident":
                return("Dr. " + re.split(r', ?', name)[0])
            else:
                return(None)
        # Otherwise if there's a space in the first name and only one character
        # after the space, we don't take the character
        elif len(re.split(" ", re.split(r', ?', name)[1])) > 1:
            if len(re.split(" ", re.split(r', ?',  name)[1])[1]) < 2:
                return(re.split(" ", re.split(r', ?', name)[1])[0])
        else:
            return(re.split(r', ?', name)[1].capitalize())

    # If no firstname, we have to check whether or not to call them doctor
    else:
        if role == "physician" or role == "Resident":
            return("Dr. " + name)
        # Otherwise we have no reliable way of referring to them correctly
        else:
            print("bye")
            return(None)


# Next 3 functions responsible for sending out texts #
def check_for_jobs(listofjobs):
    """Check for jobs to send."""
    global survey_link
    if listofjobs[0][1] <= datetime.now():
        # If the shift ended over an hour ago we pop it
        if datetime.now() - listofjobs[0][1] > timedelta(hours=1, minutes=15):
            listofjobs.pop(0)
        else:
            schedule_text(listofjobs[0][0], survey_link,
                          listofjobs[0][2], listofjobs[0][3])
            listofjobs.pop(0)
            time.sleep(0.5)
        if listofjobs:
            check_for_jobs(listofjobs)


def schedule_text(phone, survey_link, role, name):
    """Schedules texts.

    Looks to see if it is a doctor's turn to get a survey, and calls send_text
     if it is.
    """
    global text_scheduler_df
    if text_scheduler_df[text_scheduler_df['phone'] == phone].shape[0]:
        n = (text_scheduler_df.loc[text_scheduler_df.phone == phone, 'n'].
             iloc[0])
        if n < n_shifts_between_texts:
            text_scheduler_df.loc[text_scheduler_df.phone == phone, 'n'] += 1
            print("It's been " + str(int(n + 1)) + " shifts since " +
                  str(phone) + " was last texted.\n")
        else:
            text_scheduler_df.loc[text_scheduler_df.phone == phone, 'n'] = 0
            send_text(phone, survey_link, role, name)
    else:
        text_scheduler_df = text_scheduler_df.append({'phone': phone,
                                                      'n': 0},
                                                     ignore_index=True)
        print("New number added to list\n")
        send_text(phone, survey_link, role, name)


def compose_body(survey_link, role, name):
    """Create string for text with appropriate signoff for department."""
    if role == "Tech/Nurse":
        signoff = ""
    else:
        signoff = ""
    if name:
        message = ""
        if message == "":
            Exception("You are attempting to send a blank message.\n\n"
                      "Update the message variable in Script_Scheduler.py")
        return(message)
    else:
        message = ""
        if message == "":
            Exception("You are attempting to send a blank message.\n\n"
                      "Update the message variable in Script_Scheduler.py")
        return(message)


# Need to update from field in clent.messages.create function
# to a phone number that you own.
def send_text(phone_number, survey_link, role, name):
    """Send texts."""
    global DNClist
    global textlog
    global debug_mode
    try:
        print("Sending text to " + phone_number + "\n" +
              str(datetime.now()) + "\n")
        if not debug_mode:
            client.messages.create(
                to=phone_number,
                from_="",
                body=compose_body(survey_link, role, name))

        textlog = textlog.append({'phone': phone_number,
                                  'time_sent': datetime.now()},
                                 ignore_index=True)
        print("Text successfully sent")
        # Here we are going to check if the number we successfully sent to is
        # in our DNC database and take them out
        try:
            DNClist = DNClist.remove(phone_number)
        except Exception:
            # Do nothing if they weren't on the DNC list
            pass
    except TwilioRestException:
        print("There was an exception sending to " + phone_number + "\n " +
              str(datetime.now()) + "\n")
        # Check if that number is in our DNC database and if its not we add it
        if phone_number in DNClist['phone']:
            print("they are already logged as being on the DNC list\n\n")
        else:
            DNClist = DNClist.append({'phone': phone_number,
                                      'area': '',
                                      'name': '',
                                      'role': ''},
                                     ignore_index=True)
    except Exception:
        print("There was an unexpected exception sending to " +
              phone_number + "\n")
        
if (account_sid == "ENTER SID HERE" or
        auth_token == "ENTER AUTH TOKEN HERE"):
    Exception("Please enter in your account sid and auth token into the Script_Scheduler.py script.")
        