
# Filter modified date: 12/02/2025       SN:049BN6KZ01
#
# Notes: Windows 11
#
# examples
# [^\\\\]+ match up to next directory  and ends with \\LocalCache
# \\.*?\\ non greedily match up to and including first \\LocalCache
#
# any filtername with a . example randonmfile\\.txt
#

_filter = [

    # add directories

    # examples
    # r'C:\\Users\\{{user}}\\AppData\\Programs\\Recentchgs\\recent',
    # r'C:\\Users\\{{user}}\\AppData\\Programs\\Recentchgs\\flth\\.csv',

    r'C:\\.Trash-0',
    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\[^\\]+\\LocalCache',
    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\.*?\\LocalCache'

    #      Now we get into the important directories. Do we exclude at the risk missing something? its better to include than
    #       exclude
    #
]


# filter hits to reset on cache clear in ftlh.csv in app install. copy from filter items above
_filterhitRESET = [

    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\[^\\]+\\LocalCache',
    r'C:\\Users\\{{user}}\\AppData\\Local\\Packages\\.*?\\LocalCache'

]
