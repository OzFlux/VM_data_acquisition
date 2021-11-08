# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import ephem
from pytz import timezone

def get_sunrise_sunset_utc(lat, lon, elev, date):
    
    """Get the local sunrise and sunset times (UTC)"""
    
    obs = ephem.Observer()
    obs.lat = lat
    obs.long = lon
    obs.elev = elev
    obs.date = date
    sun = ephem.Sun()
    sun.compute(obs)
    return {'rising': obs.next_rising(sun).datetime(),
            'setting': obs.next_setting(sun).datetime()}

def get_sunrise_sunset_local(lat, lon, elev, date, tz, dst=False):
    
    """Get the local sunrise and sunset times (local standard or daylight)"""
    
    utc_offset = get_timezone_utc_offset(tz=tz, date=date, dst=dst)
    utc_rslt = get_sunrise_sunset_utc(lat, lon, elev, date)
    return {'rising': utc_rslt['rising'] + utc_offset,
            'setting': utc_rslt['setting'] + utc_offset}
    
def get_timezone_utc_offset(tz, date, dst=False):
    
    """Get the UTC offset (local standard or daylight)"""
    
    tz_obj = timezone(tz)
    utc_offset = tz_obj.utcoffset(date)
    if not dst:
        return utc_offset - tz_obj.dst(date)
    return utc_offset