import re
import datetime
import dateutil
import uuid

# ======================================================================

class ErrorMsg( Exception ):
    def __init__( self, text="error" ):
        self.text = text

# ======================================================================

def sanitizeFilename( filename ):
    fname = re.sub( '[^A-Za-z0-9\.\-_]', '_', filename )
    if len(fname) == 0:
        fname = "empty_filename"
    return fname


# ======================================================================

def sanitizeHTML(text, oneline = False):
    tagfinder = re.compile("^\<(\S+)\>$")
    ampfinder = re.compile("\&([^;\s]*\s)")
    ampendfinder = re.compile("\&([^;\s]*)$")
    ltfinder = re.compile("<((?!a\s*href)[^>]*\s)")
    ltendfinder = re.compile("<([^>]*)$")
    gtfinder = re.compile("((?<!\<a)\s[^<]*)>")
    gtstartfinder = re.compile("^([<]*)>");
    
    def tagfilter(text):
        tagfinder = re.compile("^\<(\S+)\>$")
        # linkfinder = re.compile("^\s*a\s+\"[^\"]+\"\s*")     # I think this didn't work
        linkfinder = re.compile("^\s*a\s+href\s*=\s*\"[^\"]+\"\s*((target|style)\s*=\s*\"[^\"]*\"\s*)*")
        imgfinder = re.compile("^\s*img\s+((src|style|width|height|alt)\s*=\s*\"[^\"]*\"\s*)*$")
        match = tagfinder.match(text)
        if match is None:
            return None
        contents = match.group(1)
        if linkfinder.match(contents) is not None:
            return text
        if imgfinder.match(contents) is not None:
            return text
        if ( (contents.lower() == "i") or (contents.lower() == "b") or (contents.lower() == "tt") ):
            return text
        elif ( (contents.lower() == "/i") or (contents.lower() == "/b") or
               (contents.lower() == "/tt") or (contents.lower() == "/a") ):
            return text
        elif contents.lower() == "sup":
            return "<span class=\"sup\">"
        elif contents.lower() == "/sup":
            return "</span>"
        elif contents.lower() == "sub":
            return "<span class=\"sub\">"
        elif contents.lower() == "/sub":
            return "</span>"
        else:
            return "&lt;{}&rt;".format(contents)

    # sys.stderr.write("text is \"{}\"\n".format(text.encode('utf-8')))
    newtext = tagfinder.sub(tagfilter, text)
    # sys.stderr.write("after tagfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = ampfinder.sub("&amp;\g<1>", newtext, count=0)
    # sys.stderr.write("after ampfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = ampendfinder.sub("&amp;\g<1>", newtext, count=0)
    # sys.stderr.write("after ampendfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = ltfinder.sub("&lt;\g<1>", newtext, count=0)
    # sys.stderr.write("after ltfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = ltendfinder.sub("&lt;\g<1>", newtext, count=0)
    # sys.stderr.write("after ltendfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = gtfinder.sub("\g<1>&gt;", newtext, count=0)
    # sys.stderr.write("after gtfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))
    newtext = gtstartfinder.sub("\g<1>&gt;", newtext, count=0)
    # sys.stderr.write("after gtendfinder, text is \"{}\"\n".format(newtext.encode('utf-8')))

    if oneline:
        pass   # I hope I don't regret this
    else:
        newtext = re.sub("^(?!\s*<p>)", "<p>", newtext, count=0)
        # sys.stderr.write("after beginning <p>, text is \"{}\"\n".format(newtext.encode('utf-8')))
        newtext = re.sub("([^\n])$", "\g<1>\n", newtext, count=0)
        # sys.stderr.write("after ending newline, text is \"{}\"\n".format(newtext.encode('utf-8')))
        newtext = re.sub("\s*\n", "</p>\n", newtext, count=0)
        newtext = re.sub("</p></p>", "</p>", newtext, count=0)
        # sys.stderr.write("after line-end </p>, text is \"{}\"\n".format(newtext.encode('utf-8')))
        newtext = re.sub("\n(?!\s*<p>)([^\n]*</p>)", "\n<p>\g<1>", newtext, count=0)
        # sys.stderr.write("after line-start <p>, text is \"{}\"\n".format(newtext.encode('utf-8')))
        newtext = re.sub("^\s*<p></p>\s*$", "", newtext, count=0)
        # sys.stderr.write("after <p></p>, text is \"{}\"\n".format(newtext.encode('utf-8')))
        newtext = re.sub("\n", "\n\n", newtext, count=0)
    
    return newtext;

# ======================================================================

def intOrZero( val ):
    try:
        return int( val )
    except ValueError:
        return 0

def intOrError( val, description ):
    try:
        return int( val )
    except ValueError:
        raise ErrorMsg( "Error, {} needs to be an integer, got \"{}\"".format( description, val ) )


# ======================================================================

NULLUUID = uuid.UUID( '00000000-0000-0000-0000-000000000000' )

def asUUID( val, canbenone=True ):
    if val is None:
        if canbenone:
            return None
        else:
            return NULLUUID
    if isinstance( val, uuid.UUID ):
        return val
    else:
        return uuid.UUID( val )

# ======================================================================

def asDateTime( string ):
    try:
        if string is None:
            return None
        if isinstance( string, datetime.datetime ):
            return string
        dateval = dateutil.parser.parse( string )
        return dateval
    except:
        raise ErrorMsg( f'Error, {string} is not a valid date and time.' )


