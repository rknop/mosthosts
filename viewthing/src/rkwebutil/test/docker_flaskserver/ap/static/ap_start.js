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

import { exampleap } from "./ap.js";

exampleap.started = false;
exampleap.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !exampleap.started ) {
                exampleap.started = true;
                window.clearInterval( exampleap.init_interval );
                renderer = new exampleap();
                renderer.init();
            }
        }
    },
    100 );
