def obj_dict(obj):
    return obj.__dict__

# This function truncate string if its length is longger than the given length and add '...' to the end of it
# If the string is shorter than the given length, return the string
def truncate_string(str, length):
    if (len(str) > length):
        return str[0:length].strip() + "..."
    return str