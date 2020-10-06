#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 12:42:49 2020

@author: RobertTeresi
"""
import Script_Scheduler
# Link to survey below
survey_link = "https://bit.ly/yaletaskforce3"

listofjobs = []

while 1:
    ##
    if listofjobs:  # Only do this if there are jobs

        if datetime.now() < datetime(2020, 4, 28, 5, 0, 0, 0):
            time.sleep((datetime.now() - datetime(2020, 4, 28, 12, 0, 0, 0)).
                       seconds)

        check_for_jobs(listofjobs)  # This also executes jobs if they are ready

        if listofjobs:
            if listofjobs[0][1] > datetime.now():

                try:
                    print("The time now is " + datetime.now().strftime("%H:%M"))
                    print("Next job at " + listofjobs[0][1].strftime("%H:%M"))
                except:
                    pass
                if not debug_mode:
                    threading.Thread(target=update_data).start()

                # Sleep until the next job
                pause.until(listofjobs[0][1] + timedelta(seconds = .5))

            else:
                pass
    # Otherwise we check for new data!
    else:
        print("No jobs found")
        while not listofjobs:
            # Run the scraper
            if need_to_update:
                scrape_shifts()

                # Check if script is scheduled to be stopped for maintenance
                with open((str(Path.home()) + "/Dropbox/CoViD_ED_TF/"
                           "stop_script.pkl"), 'rb') as f:
                    stop_script = pickle.load(f)

                # Exit script if we want it to do so
                if stop_script:
                    exit()

                time.sleep(10)  # Give it some time
            try:
                shift_df = pd.read_csv(str(Path.home()) +
                                       "/Dropbox/CoViD_ED_TF/" +
                                       "shift_df.csv", index_col=0)

                # Make sure that phone numbers are all same type (string)
                shift_df['phone'] = shift_df['phone'].apply(str)

                try:
                    for index, row in shift_df.iterrows():
                        print(row['role'])
                        print(row['time'])
                        print(row['phone'])
                        listofjobs.append((phone_format(row['phone']),
                                           time_format(row['time'],
                                                       normal_mode),
                                           row['role'],
                                           name_format(row['name'],
                                                       row['role'])))

                    # Sort jobs by time and ensure shifts aren't double-counted
                    sort_jobs()

                    # Write tuple to csv for access while code is active
                    with open('listofjobs.csv', 'w') as file:
                        file.write('phone,time,role,name\n'
                                   '\n'.join('{},{},{},{}'.
                                             format(x[0], x[1], x[2], x[3])
                                             for x in listofjobs))

                    need_to_update = True
                except Exception:
                    print("problem appending to list")
                    exit()
            except Exception:
                print("No jobs as of " + str(datetime.now()))