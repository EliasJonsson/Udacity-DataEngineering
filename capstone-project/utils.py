import re

def read_sas_description(fn):
    '''Reads SAS description file and converts the data into dictionary of code -> name for each field.'''
    def rm_ws(s):
        return "".join(s.split())
    
    with open(fn) as f:
        sas_desc_data = f.readlines()
        
    field_regex = r"/\* (\w+( & \w+)*) - (.*)"
    value_regex = r"((\d+)|('\w+'))\s+=(.*)"
    
    label_desc_map = {}
    for row in sas_desc_data:
        if (row.strip() == ''):
            continue
        if(re.match(field_regex, row)):
            # Field and description starts and ends
            fields = rm_ws(re.search(field_regex, row).group(1)).split("&")
            for f in fields:
                label_desc_map[f] = {}
            
        if re.match(value_regex, row.strip()):
            # Collect code -> name for each field.
            value_match = re.search(value_regex, row.strip())
            key = value_match.group(1).replace('\'','')
            value = value_match.group(4).replace('\'','').strip()
            if "No PORT Code" in value:
                value = "No PORT Code"
            if "No Country Code" in value:
                value = "No Country Code"
            if (re.match(r"\d+", key)):
                try:
                    key = int(key)
                except:
                    pass
            for f in fields:
                if f == "I94PORT":
                    city_state = value.split(',')
                    value = (",".join(city_state[0:len(city_state) -1]), city_state[-1].strip()[:2])
                label_desc_map[f][key] = value
    return label_desc_map
