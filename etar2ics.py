#!/usr/bin/env python3

import sqlite3
import icalendar
import datetime 
import pytz
import uuid
import dateparser


from dateutil.tz import UTC
from dateutil.rrule import rrulestr, rruleset

from sqlite3 import Error
from icalendar import Calendar, Event
from datetime import datetime
from pytz import UTC # timezone
from datetime import date
from datetime import time
from datetime import timedelta
from icalendar.parser import Parameters


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def assignrdateifpossible(event, value):
    if value and value != 'None':
        # print("rdate : " + value)
        # event.add(key, datetime.utcfromtimestamp(value))
        tmp = []
        for i in value.split(','):
            tmp.append(dateparser.parse(i, date_formats=["%Y%m%dT%H%M%SZ","%Y%m%dT%H%M%S", "%Y%m%d"]))


        tmp.pop(0)
        # print("rdate : " + str(tmp))
        event.add('rdate', tmp )
    return event

def assignifpossible(event, key, value):
    if value and value != 'None':
        # print(key + ": " + value)
        # event.add(key, datetime.utcfromtimestamp(value))
        event.add(key, value)
    return event
    
def parse_rrule(rrule_str):
    rrule = {}
    if rrule_str and rrule_str != 'None':
        # print("rrule_str: " +  rrule_str)
        for val in rrule_str.split(';'):
            print(val.split('='))
            v = val.split('=')
            if v[0] == 'UNTIL': # it would be much easier if the time format was specified. Since it does not seem to be, we need to try parsing different common formats
                tmp = dateparser.parse(v[1], date_formats=["%Y%m%dT%H%M%SZ",  "%Y%m%dT%H%M%S", "%Y%m%d"])
                v[1] = tmp
            if v[0] == 'BYDAY':
                 tmp = list(map(icalendar.vWeekday, v[1].split(',')))
                 v[1] = tmp
            rrule[v[0]] = v[1]
    
    return rrule


def select_all_tasks(conn):
    """
    Query all rows in the events table and output as ics
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("Select * from Calendars")
    calendar_rows = cur.fetchall()


    for calendar_row in calendar_rows:
        calendar_name = str(calendar_row[0])
        cur.execute("SELECT * FROM view_events where calendar_id=" +
                    calendar_name)
        rows = cur.fetchall()
        cal = Calendar()
        cal.add('prodid', '-//My calendar product//mxm.dk//')
        cal.add('version', '2.0')

        for row in rows:
            debugcal = Calendar()
            debugcal.add('prodid', '-//My calendar product//mxm.dk//')
            debugcal.add('version', '2.0')
            event = Event()
            title = str(row[1])
            location = str(row[3])
            if row[8]:
                dtstart = int(row[8]/1000)
                tmp = datetime.fromtimestamp(dtstart, pytz.utc)
                event.add('dtstart', tmp)
                print('dtstart: ' + str(tmp))

            if row[9]:
                dtend = int(row[9]/1000)
                event.add('dtend',  datetime.utcfromtimestamp(dtend))

# columns in view_events
# 0  _id
# 1  title
# 2  description
# 3  eventLocation
# 4  eventColor
# 5  eventColor_index
# 6  eventStatus
# 7  selfAttendeeStatus
# 8  dtstart
# 9 dtend
# 10 duration
# 11 eventTimezone
# 12 eventEndTimezone
# 13 allDay
# 14 accessLevel
# 15 availability
# 16 hasAlarm
# 17 hasExtendedProperties
# 18 rrule
# 19 rdate
# 20 exrule
# 21 exdate
# 22 original_sync_id
# 23 original_id
# 24 originalInstanceTime
# 25 originalAllDay
# 26 lastDate
# 27 hasAttendeeData
# 28 calendar_id
# 29 guestsCanInviteOthers
# 30 guestsCanModify
# 31 guestsCanSeeGuests
# 32 organizer
# 33 isOrganizer
# 34 customAppPackage
# 35 customAppUri
# 36 uid2445
# 37 data1,sync_data2,sync_data3,sync_data4,sync_data5,sync_data6,sync_data7,sync_data8,sync_data9,sync_data10,
# 47 comment
# 48 deleted
# 49 _sync_id
# 50 dirty
# 51 mutators
# 52 lastSynced
# 53 account_name
# 54 account_type
# 55 calendar_timezone
# 56 calendar_displayName
# 57 calendar_location,visible,calendar_color,calendar_color_index,calendar_access_level,maxReminders,allowedReminders
# 64 allowedAttendeeTypes,allowedAvailability,canOrganizerRespond,canModifyTimeZone,canPartiallyUpdate
# 69 cal_sync1,cal_sync2,cal_sync3,cal_sync4,cal_sync5,cal_sync6,cal_sync7,cal_sync8,cal_sync9,cal_sync10
# 79 ownerAccount,sync_events,displayColor)

            if row[10]:
                event.add('event_timezone', row[10])
            event.add('event_timezone', row[11])
            # event.add('duration', row[10])
            event.add('allday', row[13])
            print ("calendar_row: " + str(row))
            event.add('summary', title)
            if row[2]:
                event.add('description', row[2])
            
            event.add('dtstamp', datetime(2020,11,14,0,10,0,tzinfo=UTC)) # creation time of event
            event = assignrdateifpossible(event, row[19])
            event = assignifpossible(event, 'comment', row[47])
            if row[18]:
                # print("row 18: " + row[18])
                event.add('rrule', parse_rrule(row[18]))

            if row[36]:
                event['uid'] = row[36]
            else:
                event['uid'] = uuid.uuid4()

            event.add('priority', 5)
            print(event)

            cal.add_component(event)
            debugcal.add_component(event)

            df = open(str(event['uid']) + '.ics', 'wb')
            df.write(debugcal.to_ical())
            df.close()
        f = open(str(calendar_row[6]).replace("/","") + '.ics', 'wb')
        f.write(cal.to_ical())
        f.close()

def main():
    database = r"/tmp/calendar.db"
    conn = create_connection(database)
    with conn:
        select_all_tasks(conn)

if __name__ == '__main__':
    main()
