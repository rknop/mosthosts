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

import { rkAuth } from "./rkauth.js"

const urlmatch = new RegExp( '^(https?://[^/]*)?(.*)/auth/resetpassword\\?uuid=[0-9a-f\-]+$' )
var loc = String( window.location );
var match = String(window.location).match( urlmatch );
if ( match == null ) {
    window.alert( "Error!  Failed to parse URL" + window.location );
    die( "Reset password broken." );
}
const webapurl = match[2];

rkAuth.started = false;
rkAuth.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !rkAuth.started ) {
                rkAuth.started = true;
                window.clearInterval( rkAuth.init_interval );
                let div = document.getElementById( "authdiv" );
                let auther = new rkAuth( div, webapurl, null );
                let button = document.getElementById( "setnewpassword_button" );
                button.addEventListener( "click", function() { auther.getPrivKey() } );
            }
        }
    },
    100);
            
    
