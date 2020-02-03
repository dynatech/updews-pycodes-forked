import sys,re
import pandas as pd
import numpy as np
import dynadb.db as dynadb
from datetime import datetime as dt
import smsclass
import volatile.memory as mem

SURFICIAL_PARSER_ERROR_VALUE = {
    "obv_type": 1,
    "site_code": 2,
    "date_conversion": 3,
    "date_no_matches": 4,
    "date_value_advance": 5,
    "measurement_no_matches": 6,
    "time_conversion": 7,
    "time_no_matches": 8,
    "time_out_of_bounds": 9,
    "weather_no_match": 10,
    "names_no_matches": 11,
    "re_parsing_error": 12
}

def get_obv_type(text):
    """
    - The processing of getting data of observer type.

    :param text: Sms line of message for soms .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0

    OBV_TYPES_REGEX_TEXT = {
        "^ROUTINE\W+": "ROUTINE",
        "^R0UTINE\W+": "ROUTINE",
        "^EVENT\W+": "EVENT",
        "^EVNT\W+": "EVENT",
    }

    obv_type = None
    match = None
    for pattern in OBV_TYPES_REGEX_TEXT.keys():
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            obv_type = OBV_TYPES_REGEX_TEXT[pattern]
            match = match.group(0)
            break

    if not match:
        err_val = SURFICIAL_PARSER_ERROR_VALUE['obv_type']

    return {"value": obv_type, "match": match, "err_val": err_val}

def get_site_code(text):
    """
    - The processing of getting data of site code.

    :param text: Sms line of message for soms .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0
    site_id = 0
    mc = mem.get_handle()

    site_code_match = re.split(" ", text, maxsplit = 1)[0].lower()[0:3]
    sites_dict = mc.get('sites_dict')
    site_code = adjust_site_code(site_code_match)
    try:
        site_id = sites_dict['site_id'][site_code]
    except KeyError:
        print "No site_code record for %s" % (site_code)
        err_val = SURFICIAL_PARSER_ERROR_VALUE["site_code"]

    return {"value": site_id, "match": str(site_code_match), "err_val": err_val}

def adjust_site_code(site_code):
    """
    - The processing of getting data of  adjusted site code.

    :param site_code: 3 letter site code of sensor.
    :type site_code: str

    Returns:
       dict: Dictionary list of site that adjusted if succesful and
       site code name fails.
    """
    translation_dict = {
        'man':'mng','bat':'bto','tag':'tga','pan':'png','pob':'jor'
    }

    if site_code in translation_dict.keys():
        return translation_dict[site_code]
    else:
        return site_code

def get_date(text):
    """
    - The processing of getting date.

    :param text: Sms line of message for soms .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
  # timetxt = ""

    err_val = 0

    MON_RE1 = "[JFMASOND][AEPUCO][NBRYLGTVCP]"
    MON_RE2 = "[A-Z]{4,9}"
    DAY_RE1 = "(([0-3]{0,1})([0-9]{1}))"
    YEAR_RE1 = "((20)([12]{0,1})([1234567890]){0,1}))"

    cur_year = str(dt.today().year)

    SEPARATOR_RE = "[\. ,:]{0,3}"
    DATE_FORMAT_DICT = {
        MON_RE1 + SEPARATOR_RE + DAY_RE1 + SEPARATOR_RE + YEAR_RE1 : "%b%d%Y",
        DAY_RE1 + SEPARATOR_RE + MON_RE1 + SEPARATOR_RE + YEAR_RE1 : "%d%b%Y",
        MON_RE2 + SEPARATOR_RE + DAY_RE1 + SEPARATOR_RE + YEAR_RE1 : "%B%d%Y",
        DAY_RE1 + SEPARATOR_RE + MON_RE2 + SEPARATOR_RE + YEAR_RE1 : "%d%B%Y",
        MON_RE1 + SEPARATOR_RE + DAY_RE1 + SEPARATOR_RE + cur_year : "%b%d%Y",
        DAY_RE1 + SEPARATOR_RE + MON_RE1 + SEPARATOR_RE + cur_year : "%d%b%Y",
        MON_RE2 + SEPARATOR_RE + DAY_RE1 + SEPARATOR_RE + cur_year : "%B%d%Y",
        DAY_RE1 + SEPARATOR_RE + MON_RE2 + SEPARATOR_RE + cur_year : "%d%B%Y"
    }

    DATE_FORMAT_STD = "%Y-%m-%d"

    match = None
    match_date_str = None
    date_str = None
    for fmt in DATE_FORMAT_DICT:
        match = re.search("^" + fmt,text)
        if match:
            match_date_str = match.group(0)
            date_str = re.sub("[^A-Z0-9]","", match_date_str).strip()
            try:
                date_str = dt.strptime(date_str,
                    DATE_FORMAT_DICT[fmt]).strftime(DATE_FORMAT_STD)
            except ValueError:
                err_val = SURFICIAL_PARSER_ERROR_VALUE["date_conversion"]
                date_str = None
            break

    # no match detected
    if match is None or date_str == None:
        err_val = SURFICIAL_PARSER_ERROR_VALUE["date_no_matches"]

    elif dt.strptime(date_str, DATE_FORMAT_STD) > dt.now():
        err_val = SURFICIAL_PARSER_ERROR_VALUE["date_value_advance"]

    return {"value": str(date_str), "match": str(match_date_str), 
            "err_val": err_val}

def get_time(text):
    """
    - The processing of getting time.

    :param text: Sms line of message from community .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0
  # timetxt = ""
    HM = "\d{1,2}"
    SEP = " *:+ *"
    DAY = " *[AP]\.*M\.*"
  
    TIME_FORMAT_DICT = {
        HM + SEP + HM + DAY : "%I:%M%p",
        HM + DAY : "%I%p",
        HM + SEP + HM + " *N\.*N\.*" : "%H:%M",
        HM + SEP + HM + " +" : "%H:%M"
    }

    time_str = ''
    match = None
    match_time_str = None
    TIME_FORMAT_STD = "%H:%M:%S"

    for fmt in TIME_FORMAT_DICT:
        match = re.search(fmt, text)
        if match:
            match_time_str = match.group(0)
            time_str = re.sub(";",":", match_time_str)
            time_str = re.sub("[^APM0-9:]","", time_str)

            try:
                time_str = dt.strptime(time_str,
                    TIME_FORMAT_DICT[fmt]).strftime(TIME_FORMAT_STD)
            except ValueError:
                err_val = SURFICIAL_PARSER_ERROR_VALUE["time_conversion"]
            break

    if match is None:
        err_val = SURFICIAL_PARSER_ERROR_VALUE["time_no_matches"]
      
    # sanity check
    if not err_val:
        time_val = dt.strptime(time_str, TIME_FORMAT_STD).time()
        if (time_val > dt.strptime("18:00:00","%H:%M:%S").time() or time_val < 
            dt.strptime("05:00:00","%H:%M:%S").time()):
            print 'Time out of bounds. Unrealizable time to measure' 
            err_val = SURFICIAL_PARSER_ERROR_VALUE["time_out_of_bounds"]

    return {"value": str(time_str), "match": str(match_time_str), 
            "err_val": err_val}

def get_measurements(text):
    """
    - The processing of getting measurement.

    :param text: Sms line of message from community .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0
    text = " " + text

    pattern = "(?<= )[A-Z] *\d{1,3}\.*\d{0,2} *C*M"
    measurement_matches = re.findall(pattern, text)

    if not measurement_matches:
        err_val = SURFICIAL_PARSER_ERROR_VALUE["measurement_no_matches"]

    return {"value": measurement_matches, "match": measurement_matches, 
        "err_val": err_val}

def get_weather_description(text):
    """
    - The processing of getting weather description.

    :param text: Sms line of message from community .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0
    match = None
    match_str = None

    KEYWORDS = ["ARAW","ULAN","BAGYO","LIMLIM","AMBON","ULAP","SUN",
        "RAIN","CLOUD","DILIM","HAMOG","INIT"]

    for keyword in KEYWORDS:
        pattern = "((?<= )|(?<=^))[A-Z]*" + keyword + "[A-Z]*(?= )"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            match_str = match.group(0)
            break

    if match is None:
        err_val = SURFICIAL_PARSER_ERROR_VALUE["weather_no_match"]
    else:
        match_str = match_str[:20]

    return {"value": match_str, "match": match_str, "err_val": err_val}

def get_observer_names(text):
    """
    - The processing of getting observer name.

    :param text: Sms line of message from community .
    :type text: str

    Returns:
       list: List data output for success parsing and it break
       if fails.
    """
    err_val = 0

    NAME_PATTERN = "((?<= )|(?<=^))[A-Z]{4,}((?= )|(?=$)|(?=\.$))"

    matches_list = list(re.finditer(NAME_PATTERN, text))
    names = []
    value = ""
    if len(matches_list) > 0:
        for match in matches_list:
            name = match.group(0)
            names.append(name)
            value += name + " "
        value.rstrip()
        value = value[:100]
    else:
        err_val = SURFICIAL_PARSER_ERROR_VALUE["names_no_matches"]

    return {"value": value, "match": names, "err_val": err_val}    

def find_match_in_text(match_func, text):
    """
    - The finding error match text.

    :param match_func: Sms line of message from community.
    :param text: Sms line of message from community.
    :type match_func: str
    :type text: str


    Returns:
       list and str : List data output and usparsed text 
       for success parsing and it break if fails.
    """
    match_result = match_func(text)

    if match_result["err_val"] > 0 or match_result["match"] is None:
        print "Error: %d" % match_result["err_val"]
        raise ValueError(match_result["err_val"])

    if type(match_result["match"]).__name__ == 'list':
        text_unparsed = text
        for match in match_result["match"]:
            text_unparsed = re.sub(pattern = str(match), repl = "", 
                string = text_unparsed, flags = re.IGNORECASE, count=1)         
    else:
        try:
            text_unparsed = re.sub(pattern = match_result["match"], repl = "", 
                string = text, flags = re.IGNORECASE, count=1)
        except re.error:
            print "re conversion error"
            raise ValueError(SURFICIAL_PARSER_ERROR_VALUE["re_parsing_error"])

    return match_result["value"], text_unparsed.lstrip()

def get_marker_measurements(pattern_matches):
    """
    - The getting marker measurement.

    :param pattern_matches: Measurement value from text.
    :type pattern_matches: str


    Returns:
        str : Data records output and usparsed text 
       for success parsing and it break if fails.
    """
    data_records = dict()

    for match in pattern_matches:
        marker_name = match[0]
        marker_len = float(re.search("\d{1,3}\.*\d{0,2}",match[1:]).group(0))

        # check unit
        unit_cm = re.search("\d *CM",match[1:])
        if unit_cm:
            multiplier = 1.00
        else:
            multiplier = 100.00

        marker_len = marker_len * multiplier
        data_records[marker_name] = marker_len

    return data_records

def observation(text):
    """
    - The processing of getting observer name.

    :param text: Sms line of message from community .
    :type text: str

    Returns:
       str: Marker measurement data output for success
        parsing.
    """
    obv = {}
    markers = {}
    
    text = re.sub(" +", " ", text.upper())
    text = re.sub("\.+", ".", text)
    text = re.sub(";", ":", text)
    text = re.sub("\n", " ", text)
    text = text.strip()

    # find values in patterns
    obv["meas_type"], text = find_match_in_text(get_obv_type, text)
    obv["site_id"], text = find_match_in_text(get_site_code, text)
    date_str, text = find_match_in_text(get_date, text)
    time_str, text = find_match_in_text(get_time, text)
    measurement_matches, text = find_match_in_text(get_measurements, text)
    obv["weather"], text = find_match_in_text(get_weather_description, text)
    obv["observer_name"], text = find_match_in_text(get_observer_names, text)
    
    obv['reliability'] = 1
    obv['data_source'] = 'SMS'
    obv['ts']= "%s %s" % (date_str, time_str)

    markers["measurements"] = get_marker_measurements(measurement_matches)
    
    return {"obv": obv, "markers": markers}


    
