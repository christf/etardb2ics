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

def assignifpossible(event, key, value):
    if value and value != 'None':
        print(key + ": " + value)
        # event.add(key, datetime.utcfromtimestamp(value))
        event.add(key, value)
    return event
    
def parse_rrule(rrule_str):
    rrule = {}
    if rrule_str and rrule_str != 'None':
        print(rrule_str)
        for val in rrule_str.split(';'):
            print(val.split('='))
            v = val.split('=')
            if v[0] == 'UNTIL':
                tmp = dateparser.parse(v[1], date_formats=["%Y%m%dT%H%M%SZ"])
                v[1] = tmp
            rrule[v[0]] = v[1]
        print(rrule)
    
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


# 1  _id
# 2  title
# 3  description
# 4  eventLocation
# 5  eventColor
# 6  eventColor_index
# 7  eventStatus
# 8  selfAttendeeStatus
# 9  dtstart
# 10 dtend
# 11 duration
# 12 eventTimezone
# 13 eventEndTimezone
# 14 allDay
# 15 accessLevel
# 16 availability
# 17 hasAlarm
# 18 hasExtendedProperties
# 19 rrule
# 20 rdate
# 21 exrule
# 22 exdate
# 23 original_sync_id
# 24 original_id
# 25 originalInstanceTime
# 26 originalAllDay
# 27 lastDate
# 28 hasAttendeeData
# 29 calendar_id
# 30 guestsCanInviteOthers
# 31 guestsCanModify
# 32 guestsCanSeeGuests
# 33 organizer
# 34 isOrganizer
# 35 customAppPackage
# 36 customAppUri
# 37 uid2445
# 38 sync_data1,sync_data2,sync_data3,sync_data4,sync_data5,sync_data6,sync_data7,sync_data8,sync_data9,sync_data10,
# 48 comment
# 49 deleted
# 50 _sync_id
# 51 dirty
# 52 mutators
# 53 lastSynced
# 54 account_name
# 55 account_type
# 56 calendar_timezone
# 57 calendar_displayName
# 58 calendar_location,visible,calendar_color,calendar_color_index,calendar_access_level,maxReminders,allowedReminders
# 65 allowedAttendeeTypes,allowedAvailability,canOrganizerRespond,canModifyTimeZone,canPartiallyUpdate
# 70 cal_sync1,cal_sync2,cal_sync3,cal_sync4,cal_sync5,cal_sync6,cal_sync7,cal_sync8,cal_sync9,cal_sync10
# 80 ownerAccount,sync_events,displayColor)

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
            event = assignifpossible(event, 'rdate', row[19])
            event = assignifpossible(event, 'comment', row[47])
            if row[18]:
                print("row 18: " + row[18])
                
                rules = rruleset()
                print("dtstart: " + str(event.get('dtstart')) +  " tz: " + str(event.get('dtstart') ))
                tmp = datetime.utcfromtimestamp(dtstart)
                first_rule = rrulestr(row[18], dtstart=event.get('dtstart').dt)
                if first_rule._until and first_rule._until.tzinfo is None:
                    first_rule._until = first_rule._until.replace(tzinfo=UTC)
                print(str(first_rule))
                event.add('rrule', str(first_rule))

            if row[36]:
                event['uid'] = row[36]
            else:
                event['uid'] = uuid.uuid4()

            event.add('priority', 5)
            print(event)

            cal.add_component(event)

        f = open(calendar_row[6] + '.ics', 'wb')
        f.write(cal.to_ical())
        f.close()
            
def main():
    database = r"/tmp/calendar.db"
    conn = create_connection(database)
    with conn:
        select_all_tasks(conn)

if __name__ == '__main__':
    main()
