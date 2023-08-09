/**
 * This file is part of rkwebutil
 * 
 * rkwebutil is Copyright 2023 by Robert Knop
 * 
 * rkwebutil is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * rkwebutil is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with rkwebutil. If not, see <https://www.gnu.org/licenses/>.
 * 
 */

var rkWebUtil = {}

// **********************************************************************
// **********************************************************************
// **********************************************************************
// Utility functions

rkWebUtil.wipeDiv = function( div )
{
    if ( div != null )
        while ( div.firstChild )
            div.removeChild( div.firstChild );
}

// **********************************************************************

rkWebUtil.elemaker = function( elemtype, parent, inprops )
{
    var props = { text: null, dblclick: null, click: null, change:null, classes: null, attributes: null };
    Object.assign( props, inprops );
    var text = props.text
    var click = props.click;
    var dblclick = props.dblclick;
    var change = props.change;
    var classes = props.classes;
    var attributes = props.attributes;

    var attr;
    
    var elem = document.createElement( elemtype );
    if ( parent != null) parent.appendChild( elem );
    if ( text != null )
        elem.appendChild( document.createTextNode( text ) );
    if ( click != null )
        elem.addEventListener( "click", click );
    if ( dblclick != null )
        elem.addEventListener( "dblclick", dblclick );
    if ( change != null )
        elem.addEventListener( "change", change );
    if ( classes != null ) {
        for ( let classname of classes ) {
            elem.classList.add( classname )
        }
    }
    if ( attributes != null ) {
        for ( attr in attributes ) {
            if ( attributes.hasOwnProperty( attr ) ) {
                elem.setAttribute( attr, attributes[attr] );
            }
        }
    }
    return elem;
}

// **********************************************************************

rkWebUtil.button = function( parent, title, callback )
{
    var button = document.createElement( "input" );
    button.setAttribute( "type", "button" );
    button.setAttribute( "value", title );
    button.addEventListener( "click", callback );
    if ( parent != null ) parent.appendChild( button );
    return button;
}

// **********************************************************************
// items is a list of names; put in "-" for a HR.
// callbacks is a dict of functions that map names to callback functions.
// classes is a list of css classes for the div

rkWebUtil.popupMenu = function( items, callbacks, classes, title=null, titleclasses=[], hrclasses=[] ) {
    var menu = rkWebUtil.elemaker( "div", document.body, { "classes": classes } );
    if ( title != null ) {
        rkWebUtil.elemaker( "div", menu, { "text": title, "classes": titleclasses } );
    }
    for ( let item of items ) {
        if ( item == "-" ) {
            rkWebUtil.elemaker( "hr", menu, { "classes": hrclasses } );
        }
        else {
            if ( callbacks.hasOwnProperty( item ) ) {
                rkWebUtil.button( menu, item,
                                  function( e ) {
                                      callbacks[item]( e );
                                      menu.style.visibility = "hidden";
                                      e.stopPropagation();
                                  } );
            } else {
                rkWebUtil.button( menu, item,
                                  function( e ) {
                                      menu.style.visibility = "hidden";
                                      e.stopPropagation();
                                  } );
            }
        }
    }
    menu.addEventListener( "mouseleave", function() { menu.style.visibility = "hidden"; } );
                               
    return menu;
}


// **********************************************************************
// If I ever get a date that doesn't start "2020-07-15 07:42:00" (with
// any old character in place of the space), I'm in trouble.  Alas,
// I haven't found a reliable library routine to do this, because
// Javascript insists on dates being local.

rkWebUtil.parseStandardDateString = function( datestring )
{
    // console.log( "Trying to parse date string " + datestring );
    var year = Number( datestring.substring(0, 4) );
    var month = Number( datestring.substring(5, 7) );
    var day = Number( datestring.substring( 8, 10) );
    var hour = Number( datestring.substring( 11, 13) );
    var minute = Number( datestring.substring( 14, 16) );
    var second = Number( datestring.substring( 17, 19) );
    var date;
    date = new Date( Date.UTC( year, month-1, day, hour, minute, second ) );
    // console.log( "y=" + year + " m=" + month + " d=" + day +
    //              "h=" + hour + " m=" + minute + " s=" + second );
    // console.log( "Date parsed: " + date.toString() );
    return date;
}

// **********************************************************************

rkWebUtil.dateFormat = function(date)
{
    // console.log(" Trying to format date " + date );
    var result = "";
    result += ("0000" + date.getFullYear().toString()).substr(-4, 4);
    result += "-";
    result += ("00" + (date.getMonth()+1).toString()).substr(-2, 2);
    result += "-";
    result += ("00" + date.getDate().toString()).substr(-2, 2);
    result += " ";
    result += ("00" + date.getHours().toString()).substr(-2, 2);
    result += ":";
    result += ("00" + date.getMinutes().toString()).substr(-2, 2);
    result += ":";
    result += ("00" + date.getSeconds().toString()).substr(-2, 2);
    return result;
}

rkWebUtil.dateUTCFormat = function(date)
{
    var result = "";
    result += ("0000" + date.getUTCFullYear().toString()).substr(-4, 4);
    result += "-";
    result += ("00" + (date.getUTCMonth()+1).toString()).substr(-2, 2);
    result += "-";
    result += ("00" + date.getUTCDate().toString()).substr(-2, 2);
    result += " ";
    result += ("00" + date.getUTCHours().toString()).substr(-2, 2);
    result += ":";
    result += ("00" + date.getUTCMinutes().toString()).substr(-2, 2);
    result += ":";
    result += ("00" + date.getUTCSeconds().toString()).substr(-2, 2);
    return result;
}

rkWebUtil.validateWidgetDate = function( datestr ) {
    if ( datestr.value.trim() == "" ) {
        datestr.value = ""
        return;
    }
    let num = Date.parse( datestr.value.trim() );
    if ( isNaN(num) ) {
        window.alert( "Error parsing date " + datestr.value
                      + "\nNote that Javascript's Date.parse() doesn't seem to like AM or PM at the end; "
                      + "use 24-hour time." );
        return;
    }
    let date = new Date();
    date.setTime( num );
    // TODO : verify that both Firefox and Chrome (at least) parse this right
    // (I don't just use toISOString because the T at the middle and all the precision is ugly for the user)
    datestr.value = date.getUTCFullYear() + "-"
        + String(date.getUTCMonth()+1).padStart( 2, '0') + "-"
        + String(date.getUTCDate()).padStart( 2, '0' ) + " "
        + String(date.getUTCHours()).padStart( 2, '0') + ":"
        + String(date.getUTCMinutes()).padStart(2, '0') + ":"
        + String(date.getUTCSeconds()).padStart( 2, '0') + "+00";
}

// **********************************************************************

rkWebUtil.hideOrShow = function( widget, parameter, hideparams, showparams, displaytype="block" ) {
    if ( showparams.includes( parameter ) ) {
        widget.style.display = displaytype;
    }
    else if ( hideparams.includes( parameter ) ) {
        widget.style.display = "none";
    }
    else {
        window.alert( "Programmer error: rkWebUtil.hideOrShow unknown parameter " + parameter );
    }
}

// **********************************************************************

rkWebUtil.arrayBufferToB64 = function ( buffer ) {
    // I cannot freaking believe that javascript wants you to read files to
    //  an ArrayBuffer, but then it doesn't have a btoa function that can
    //  eat an ArrayBuffer and produce base64 encoded stuff.
    let bytebuffer = new Uint8Array( buffer )
    let nbytes = bytebuffer.byteLength;
    let blob = '';
    for ( let i = 0 ; i < nbytes ; i += 1 ) {
        blob += String.fromCharCode( bytebuffer[i] );
    }
    let b64data = btoa( blob )
    return b64data;
}

// **********************************************************************

rkWebUtil.colorIntToCSSColor = function( colint ) {
    let r = Math.floor( colint / (65536) );
    let g = Math.floor( ( colint - r * 65536 ) / 256 );
    let b = colint - r * 65536 - g * 256;
    return '#' + r.toString(16).padStart(2,'0') + g.toString(16).padStart(2,'0') + b.toString(16).padStart(2,'0');
}

// **********************************************************************
// **********************************************************************
// **********************************************************************
// Class for connecting to a given web server

rkWebUtil.Connector = function( app )
{
    this.app = app;
}

// **********************************************************************
// Utility function used in HTTP request callbacks to make
//  sure the data is in.  Returns false if the data is not in,
//  returns true if it is.  Returns null if there
//  was an error (including non-JSON response).
//
// errorhandler is a function that takes a single argument
// that argument will have property "error" which is a string message

rkWebUtil.Connector.prototype.waitForJSONResponse = function( request, errorhandler=null )
{
    var type;

    // console.log( "request.readyState = " + request.readyState +
    //              ", request.stauts = " + request.status );
    if (request.readyState === 4 && request.status === 200) {
        type = request.getResponseHeader("Content-Type");
        if (type === "application/json")
        {
            return true;
        }
        else
        {
            if ( errorhandler != null ) {
                errorhandler( { "error": "Request didn't return JSON" }  );
            }
            else {
                window.alert("Request didn't return JSON.  Everything is broken.  Panic.");
            }
            return null;
        }
    }
    else if (request.readyState == 4) {
        if ( errorhandler != null ) {
            errorhandler( { "error": "Got back HTTP status " + request.status } );
        }
        else {
            window.alert("Woah, got back status " + request.status + ".  Everything is broken.  Panic.");
        }
        return null;
    }
    else {
        return false;
    }
}

// **********************************************************************
// Utility funciton to created and send an XMLHttpRequest, and wait
// for it to be fully finished before calling the handler.
//   appcommand -- the thing after the webapp, e.g. "/getquestionset"
//   data -- an object to be converted with JSON.stringify and sent
//   handler -- a function to be called when the request has fully returned
//              it will have one argument, the data from the request
//   errorhandler -- a function that takes a single argument that will
//                   have property "error" which is a string message
//
//   finalcall --  function that has takes no arguments that will always
//                 be called (think "finally" from a try/catch/finally block).


rkWebUtil.Connector.prototype.sendHttpRequest = function( appcommand, data, handler, errorhandler=null, finalcall=null )
{
    let self = this;
    let req = new XMLHttpRequest();
    req.open( "POST", this.app + appcommand );
    req.onreadystatechange = function() { self.catchHttpResponse( req, handler, errorhandler=errorhandler,
                                                                  finalcall=finalcall ) };
    req.setRequestHeader( "Content-Type", "application/json" );
    req.send( JSON.stringify( data ) );
}

rkWebUtil.Connector.prototype.sendHttpRequestMultipartForm = function( appcommand, formdata,
                                                                       handler, errorhandler=null,
                                                                       finalcall=null )
{
    let self = this;
    let req = new XMLHttpRequest();
    req.open( "POST", this.app + appcommand );
    req.onreadystatechange = function() { self.catchHttpResponse( req, handler, errorhandler, finalcall ) };
    req.send( formdata );
}

rkWebUtil.Connector.prototype.catchHttpResponse = function( req, handler, errorhandler=null, finalcall=null )
{
    let jsonstate = this.waitForJSONResponse( req, errorhandler, finalcall );
    if ( ( jsonstate == null ) && ( finalcall != null) ) {
        finalcall();
        return;
    }
    if ( !jsonstate ) return;
    
    try {
        var statedata = JSON.parse( req.responseText );
    } catch (err) {
        window.alert( "Error parsing JSON! (" + err + ")" );
        console.trace();
        console.log( req.responseText );
        if ( finalcall != null ) finalcall();
        return;
    }
    if ( statedata.hasOwnProperty( "error" ) ) {
        if ( errorhandler != null ) {
            errorhandler( statedata );
        }
        else {
            window.alert( 'Error return: ' + statedata.error );
        }
        if ( finalcall != null ) finalcall();
        return;
    }
    handler( statedata );
    if ( finalcall != null ) finalcall();
}

// **********************************************************************
// I'm honestly not sure how I want to format text, but I THINK I
// want to have paragraphs separated by \n\n.  This strips all the <p>
// and </p> tags from the text to move to that convention.

rkWebUtil.stripparagraphtags = function(text)
{
    var verystartpar = new RegExp("^\s*<p>", "g");
    var startpar = new RegExp("\n\s*<p>", "g");
    var endpar = new RegExp("</p>\s*\n", "g");
    var veryendpar = new RegExp("</p>\s*$", "g");

    var newtext = text.replace(verystartpar, "");
    newtext = newtext.replace(startpar, "\n");
    newtext = newtext.replace(endpar, "\n");
    newtext = newtext.replace(veryendpar, "");

    return newtext;
}

// **********************************************************************

export { rkWebUtil }

