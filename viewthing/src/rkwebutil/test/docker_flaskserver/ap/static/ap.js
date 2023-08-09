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
import { rkWebUtil } from "./rkwebutil.js"

var webapurl = "";

var exampleap = function() {}

exampleap.prototype.init = function() {
    var self = this;
    
    this.statusdiv = document.getElementById( "status-div" );
    this.maindiv = document.getElementById( "main-div" );
    if ( this.statusdiv == null ) {
        window.alert( "Couldn't find status div!  This should never happen." );
        return;
    }
    this.auth = new rkAuth( this.statusdiv, webapurl,
                            function() { self.render(); },
                            function() { window.location.reload(); } );
    this.auth.checkAuth();
}

// **********************************************************************


exampleap.prototype.render = function() {
    this.show_or_prompt_login();
    rkWebUtil.wipeDiv( this.maindiv );
    if ( ! this.auth.authenticated ) {
        rkWebUtil.elemaker( "p", this.maindiv, { "text": "Not logged in." } );
    }
    else {
        rkWebUtil.elemaker( "p", this.maindiv, { "text": "Hello world." } );
    }
}

// **********************************************************************

exampleap.prototype.show_or_prompt_login = function() {
    var self = this;
    rkWebUtil.wipeDiv( this.statusdiv );
    if ( this.auth.authenticated ) {
        let p = rkWebUtil.elemaker( "p", this.statusdiv, { "classes": [ "smaller", "italic" ] } );
        p.appendChild( document.createTextNode( "Logged in as " + this.auth.username +
                                                " (" + this.auth.userdisplayname + ") — " ) );
        rkWebUtil.elemaker( "span", p,
                            { "classes": [ "link" ],
                              "text": "Log Out",
                              "click": function() {
                                  self.auth.logout( function() { window.location.reload(); } )
                              }
                            }
                          );
    }
    else {
        this.auth.showLoginUI();
    }
}
    
// **********************************************************************

export { exampleap, webapurl }
