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

var SVGPlot = {};

// **********************************************************************
// Utility function for generating a series of axis labels that are
//  nicely spaced

SVGPlot.nice = function( val )
{
    if ( val == 0 ) return 0;
    var valsign = 1;
    if ( val < 0 ) {
        valsign = -1;
        val = Math.abs( val );
    }
    var pow10 = Math.floor( Math.log( val ) / Math.log( 10 ) );
    var multiplyer = 10 ** pow10;
    var intval = Math.round( val / multiplyer )
    if ( intval > 8 ) intval = 10;
    else if ( intval > 3 ) intval = 5;
    else if ( intval >= 2 ) intval = 2;
    else intval == 1;
    return valsign * intval * multiplyer;
}

SVGPlot.generateTickValues = function( min, max, sp )
{
    if ( min > max ) {
        let temp = min;
        min = max
        max = temp;
    }

    if ( sp == null ) {
        sp = SVGPlot.nice( ( max - min ) / 5. );
    }
    var pow10 = Math.floor( Math.log( sp ) / Math.log( 10. ) );
    var nsubticks = 4;
    var spintmantissa = Math.floor( sp / ( 10 ** pow10 ) );
    if ( spintmantissa == 3 || spintmantissa == 9 )
       nsubticks = 3;
    else if ( spintmantissa == 5 )
        nsubticks = 5;
    else if ( spintmantissa == 6 )
        nsubticks = 6;
    else if ( spintmantissa == 7 )
        nsubticks = 7;
        
    var ticks = [];
    // var curtick = SVGPlot.nice( min );
    var mindivsp = Math.floor( min / sp )
    var curtick = sp * mindivsp;
    if ( curtick < min ) curtick += sp;
    while ( curtick <= max ) {
        if ( pow10 > 3 || pow10 < -2 )
            ticks.push( curtick.toExponential(1) );
        else {
            if ( pow10 > 0 ) {
                ticks.push( curtick.toFixed(0) );
            }
            else {
                ticks.push( curtick.toFixed( -pow10 + 1 ) );
            }
        }
        curtick += sp;
    }
    return { "ticks": ticks, "nsubticks": nsubticks };
}

// **********************************************************************
// SVGPlot.Plot encapsulates a single plot.  Key properties include the
// following. Things that start with "params." can be set at instantiation
// as properties of the one argument to the constructor, though without
// the "params." in their name.  (See the "params" variable in constructor.)
//    topdiv -- a div to embed.  You can put other things in here too.  Will
//              have a row of buttons at the top, then div.
//    buttonbox -- a div, the part of the top row that has buttons
//    docbox -- a div, the part of the top row that has text
//    div -- The div in which the svg plot goes  (read only)
//    svg -- The svg element.  Gets regenerated/replaced a lot (read only)
//
// CSS classes that should be defined (see svgplot.css for template, need some layout stuff to work)
//    div.svgplottopdiv -- Main div for plot interface.
//    div.svgplotdiv -- Actual plot goes in here
//    div.svgplothbox -- flex box that lays out horizontally 
//    [ div.svgplotvbox -- flex box that lays out vertically ] [ not used? ]
//    .svgplotsvg -- The actual svg.  Plot styling is done inline, as
//                   it was a nightmare trying to get external style sheets to
//                   work.  That also means the svg stands alone for plot rendering
//
// svg viewbox is always params.width x params.height, default 1024×768


SVGPlot.Plot = function( inparams = {} )
{
    this.params = { "divid": "svgplotdiv",
                    "svgid": "svgplotsvg",
                    "showerrbar": true,
                    "title": null,
                    "xtitle": null,
                    "ytitle": null,
                    "width": 1024,
                    "height": 768,
                    "left": null,
                    "right": null,
                    "bottom": null,
                    "top": null,
                    "pagemargin": 10,
                    "borderwid": 2,
                    "bordercolor": "black",
                    "borderfill": "#eeeeee",
                    "borderdash": null,
                    "axeswid": 0,
                    "axescolor": "black",
                    "axesdash": null,
                    "ticklen": 10,
                    "tickwid": 2,
                    "tickcolor": "black",
                    "subticklen": 5,
                    "subtickwid": 2,
                    "subtickcolor": "black",
                    "titlefamily": "sans-serif",
                    "titlestyle": "normal",
                    "titleweight": "bold",
                    "titlesize": 32,
                    "axistitlefamily": "serif",
                    "axistitlestyle": "italic",
                    "axistitleweight": "normal",
                    "axistitlesize": 28,
                    "axislabelfamily": "serif",
                    "axislabelstyle": "normal",
                    "axislabelweight": "normal",
                    "axislabelsize": 24,
                    "gridwid": 2,
                    "gridcolor": "#aaaaaa",
                    "zoomboxborder": "black",
                    "zoomboxcolor": "black",
                    "zoomboxopacity": 0.25,
                    "zoomboxborderwid": 4,
                    "minautoxrange": 1.,
                    "minautoyrange": 1.,
                    "equalaspect": false,
                    "nozoomdocstring": false,    // Make this "true" to suppress the "Shift+LMB to zoom" text
                    "zoommode": "full",          // "full" (show all), "default", or "manual"
                    "defaultlimits": []
                 };
    Object.assign( this.params, inparams );

    this.topdiv = document.createElement( "div" );
    this.topdiv.setAttribute( "class", "svgplottopdiv" );

    var hbox = document.createElement( "div" );
    hbox.setAttribute( "class", "svgplothbox" );
    this.topdiv.appendChild( hbox );
    this.buttonbox = document.createElement( "div" );
    this.buttonbox.setAttribute( "class", "svgplotbuttonbox" )
    hbox.appendChild( this.buttonbox );
    this.docbox = document.createElement( "div" );
    this.docbox.setAttribute( "class", "svgplotdocbox" )
    hbox.appendChild( this.docbox );
    
    var self = this;
    var button = document.createElement( "button" );
    button.appendChild( document.createTextNode( "Zoom Default" ) );
    this.buttonbox.appendChild( button );
    button.addEventListener( "click", function() { self.zoomToDefault(); } );
    button = document.createElement( "button" );
    button.appendChild( document.createTextNode( "Zoom All" ) );
    this.buttonbox.appendChild( button );
    button.addEventListener( "click", function() { self.zoomToFull(); } );
    button = document.createElement( "button" );
    button.appendChild( document.createTextNode( "Zoom Out" ) );
    this.buttonbox.appendChild( button );
    button.addEventListener( "click", function() { self.zoomOut(); } );
    
    if ( ! this.params.nozoomdocstring )
        this.docbox.appendChild( document.createTextNode( "  Shift+LMB to zoom" ) );
    
    this.clickcallback = function( event ) { self.click( event ); };
    this.downcallback = function( event ) { self.mousedown( event ); };
    this.movecallback = function ( event ) { self.mousemoved( event ); };
    this.upcallback = function( event ) { self.mouseup( event ); };

    this.div = document.createElement( "div" );
    this.div.setAttribute( "class", "svgplotdiv" );
    this.div.setAttribute( "id", this.params.divid );
    this.topdiv.appendChild( this.div );

    var ns = "http://www.w3.org/2000/svg";
    this.svg = document.createElementNS( ns, "svg" );
    // I haven't figured out how the hell to get the xmlns attribute in there.
    // Firefox yells at me if I try setAttributeNS, and it doesn't do anything
    // if I do setAttribute
    this.svg.setAttributeNS( 'http://www.w3.org/2000/xmlns/', "xmlns", ns );
    this.svg.addEventListener( "mousedown", this.downcallback );
    this.svg.addEventListener( "click", this.clickcallback );
    this.div.appendChild( this.svg );

    this.datasets = [];
    this.highlighter = null;
    this.clicklisteners = [];
    this.redrawlisteners = [];
    this._defaultlimits = [...this.params.defaultlimits];
    this._zoommode = this.params.zoommode;
    this._redrawonaddpoint = false;
    
    var self = this;

    this.resizeobs = new ResizeObserver(
        function( entries, observer ) {
            self.redraw();
        }
    );
    this.resizeobs.observe( this.div );
}

// **********************************************************************

Object.defineProperty( SVGPlot.Plot.prototype, "zoommode", {
    get() { return this._autoscale },
    set( val ) {
        if ( this._zoommode != val ) {
            this._zoommode = val;
            this.redraw();
        }
    }
} );

Object.defineProperty( SVGPlot.Plot.prototype, "defaultlimits", {
    get() { return [...this._defaultlimits]; },
    set ( val ) {
        this._defaultlimits = [...val];
        if ( this._zoommode == "default" ) {
            this.redraw();
        }
    }
} );

Object.defineProperty( SVGPlot.Plot.prototype, "xmin", {
    get() { return this._minx; },
    set( val ) {
        this._minx = val;
        this.zoommode = "manual";
        this.redraw();
    }
} );
                     
Object.defineProperty( SVGPlot.Plot.prototype, "xmax", {
    get() { return this._maxx; },
    set( val ) {
        this._maxx = val;
        this.zoommode = "manual";
        this.redraw();
    }
} );
                     
Object.defineProperty( SVGPlot.Plot.prototype, "ymin", {
    get() { return this._miny; },
    set( val ) {
        this._miny = val;
        this.zoommode = "manual";
        this.redraw();
    }
} );
                     
Object.defineProperty( SVGPlot.Plot.prototype, "ymax", {
    get() { return this._maxy; },
    set( val ) {
        this._maxy = val;
        this.zoommode = "manual";
        this.redraw();
    }
} );
                     

Object.defineProperty( SVGPlot.Plot.prototype, "title", {
    get() { return this.params.title },
    set( val ) {
        this.params.title = val;
        this.redraw();
    }
} );

Object.defineProperty( SVGPlot.Plot.prototype, "xtitle", {
    get() { return this.params.xtitle },
    set( val ) {
        this.params.xtitle = val;
        this.redraw();
    }
} );

Object.defineProperty( SVGPlot.Plot.prototype, "ytitle", {
    get() { return this.params.ytitle },
    set( val ) {
        this.params.ytitle = val;
        this.redraw();
    }
} );

// **********************************************************************

SVGPlot.Plot.prototype.zoomTo = function( minx, maxx, miny, maxy ) {
    this._minx = minx;
    this._miny = miny;
    this._maxx = maxx;
    this._maxy = maxy;
    this._zoommode = "manual";
    this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.zoomOut = function() {
    let dx = this._maxx - this._minx;
    let dy = this._maxy - this._miny;
    this.zoomTo( this._minx - dx/2., this._maxx + dx/2.,
                 this._miny - dy/2., this._maxy + dy/2. );
}

// **********************************************************************

SVGPlot.Plot.prototype.zoomToDefault = function() {
    this._zoommode = "default";
    this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.zoomToFull = function() {
    this._zoommode = "full";
    this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.addClickListener = function( listener ) {
    if ( ! this.clicklisteners.includes( listener ) ) {
        this.clicklisteners.push( listener );
    }
}

SVGPlot.Plot.prototype.removeClickListener = function( listener ) {
    const dex = this.clicklisteners.indexOf( listener );
    if ( dex >= 0 ) {
        this.clicklisteners.splice( dex, 1 );
    }
}

// **********************************************************************
// Be very careful not to write infinite loops with redraw listeners!
// In particular, if a redraw listener calls a redraw (of *any* plot),
// it's easy to accidentally do that!

SVGPlot.Plot.prototype.addRedrawListener = function( listener ) {
    if ( ! this.redrawlisteners.includes( listener ) ) {
        this.redrawlisteners.push( listener );
    }
}

SVGPlot.Plot.prototype.removeRedrawListener = function( listener ) {
    const dex = this.redrawlisteners.indexOf( listener );
    if ( dex >= 0 ) {
        this.redrawlisteners.splice( dex, 1 );
    }
}

// **********************************************************************

SVGPlot.Plot.prototype.addDataset = function( dataset )
{
    let dex = -1;
    for ( let i in this.datasets ) {
        if ( this.datasets[i] == dataset ) {
            dex = i;
            break;
        }
    }
    if ( dex >= 0 ) {
        console.log( "ERROR: tried to add a dataset to a SVGPlot that was already on the plot." )
        return;
    }
    this.datasets.push( dataset );
    dataset.plot = this;
    if ( this._redrawonaddpoint )
        this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.removeDataset = function( dataset )
{
    let dex = -1;
    for ( let i in this.datasets ) {
        if ( this.datasets[i] == dataset ) {
            dex = i;
            break;
        }
    }
    if ( dex < 0 ) {
        console.log( "ERROR: tried to remove a dataset from a SVGPlot that wasn't in that plot." );
        return;
    }
    this.datasets.splice( dex, 1 );
    this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.removeDatasetByIndex = function( dex )
{
    this.datasets.splice( dex, 1 );
    this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.clear = function( redraw = true )
{
    this.datasets = [];
    if ( redraw ) this.redraw();
}

// **********************************************************************

SVGPlot.Plot.prototype.elemSize = function( elem )
{
    var bbox = elem.getBBox();
    // var ctm = elem.getCTM();
    // var x0 = ctm.a * bbox.x + ctm.b * bbox.y;
    // var y0 = ctm.c * bbox.x + ctm.d * bbox.y;
    // var x1 = ctm.a * ( bbox.x + bbox.width ) + ctm.b * ( bbox.y + bbox.height );
    // var y1 = ctm.c * ( bbox.x + bbox.width ) + ctm.d * ( bbox.y + bbox.height );
    // // compstyle = window.getComputedStyle( elem );
    // // console.log( "font-size = " + compstyle.getPropertyValue( "font-size"  ) );
    // return { "width": x1-x0, "height": y1-y0 };
    return { "width": bbox.width, "height": bbox.height };
}


// **********************************************************************

SVGPlot.Plot.prototype.redraw = function( width=null )
{
    let self = this;
    while ( this.svg.firstChild) this.svg.removeChild( this.svg.firstChild );

    var rect;
    const ns = "http://www.w3.org/2000/svg";

    var mustcalcautoscale = false;
    if ( this._zoommode == "full" ) {
        mustcalcautoscale = true;
        this._minx = null;
        this._maxx = null;
        this._miny = null;
        this._maxy = null;
    }
    else if ( this._zoommode == "default" ) {
        this._minx = this._defaultlimits[0];
        this._maxx = this._defaultlimits[1];
        this._miny = this._defaultlimits[2];
        this._maxy = this._defaultlimits[3];
        if ( ( this._minx == null ) || ( this._maxx == null ) ||
             ( this._miny == null ) || ( this._maxy == null ) ) {
            mustcalcautoscale = true;
        }
    }
    else if ( this._zoommode == "manual" ) {
        mustcalcautoscale = false;
    }
    else {
        window.alert( "Unknown zoom mode " + this._zoommode + ", redraw fails." );
        return;
    }

    if ( mustcalcautoscale ) {
        let xmin = 1e30;
        let xmax = -1e30;
        let ymin = 1e30;
        let ymax = -1e30;
        let npoints = 0;
        for ( var dataset of this.datasets ) {
            for ( var i in dataset._x ) {
                npoints += 1;
                let x = dataset._x[i] * dataset.scalex + dataset.offx;
                let y = dataset._y[i] * dataset.scaley + dataset.offy
                if ( x < xmin ) xmin = x;
                if ( x > xmax ) xmax = x;
                if ( y < ymin ) ymin = y;
                if ( y > ymax ) ymax = y;
            }
        }
        if ( npoints == 0 ) {
            xmin = -this.params.minautoxrange/2;
            xmax =  this.params.minautoxrange/2;
            ymin = -this.params.minautoyrange/2;
            ymax =  this.params.minautoyrange/2;
        }
        else {
            let xmid = ( xmax + xmin ) / 2
            let ymid = ( ymax + ymin ) / 2;
            let dx = ( xmax-xmin ) * 1.1;
            if ( this.params.minautoxrange != null && this.params.minautoxrange > dx )
                dx = this.params.minautoxrange;
            let dy = ( ymax-ymin ) * 1.1;
            if ( this.params.minautoyrange != null && this.params.minautoyrange > dy )
                dy = this.params.minautoyrange;
            if ( dx == 0 ) dx = 1.;
            if ( dy == 0 ) dy = 1.;
            xmin = xmid - dx/2;
            xmax = xmid + dx/2;
            ymin = ymid - dy/2;
            ymax = ymid + dy/2;
        }
        if ( this._zoommode == "default" ) {
            if ( this._minx == null ) this._minx = xmin;
            if ( this._maxx == null ) this._maxx = xmax;
            if ( this._miny == null ) this._miny = ymin;
            if ( this._maxy == null ) this._maxy = ymax;
        } else {
            this._minx = xmin;
            this._maxx = xmax;
            this._miny = ymin;
            this._maxy = ymax;
        }
    }
   
    var svg = this.svg

    // This calculation is still returnin gsomething too wide, and I don't know why.
    // I fixed it by adding "width 100%" to the div (which has position: relative).
    var divstyle = window.getComputedStyle( this.div );
    var fullwidstr = divstyle.getPropertyValue( "width" );
    var fullwid = parseFloat( fullwidstr );
    var blwstr = divstyle.getPropertyValue( "border-left-width" )
    var blw = parseFloat( blwstr  );
    var brwstr = divstyle.getPropertyValue( "border-right-width" )
    var brw = parseFloat( brwstr  );
    var plwstr = divstyle.getPropertyValue( "padding-left" )
    var plw = parseFloat( plwstr  );
    var prwstr = divstyle.getPropertyValue( "padding-right" )
    var prw = parseFloat( prwstr  );
    var wid = fullwid - blw - brw - plw - prw;
    // console.log( "Div width is " + fullwid + " - " + blw + " - " + brw + " - " + plw + " - " + prw + " = " + wid );
    if ( width == null ) width = wid;
    this.svg.setAttribute( "width", width );
    this.svg.setAttribute( "height", Math.round( width * this.params.height / this.params.width ) );
    this.div.appendChild( this.svg );
    svg.setAttribute( "class", "svgplotsvg" );
    svg.setAttribute( "viewBox", "0 0 " + this.params.width + " " + this.params.height );
    svg.setAttribute( "id", this.params.svgid );

    // Markers

    var defs = document.createElementNS( ns, "defs" );
    svg.appendChild( defs );
    for ( var i = 0 ; i < this.datasets.length ; ++i )
    {
        if ( this.datasets[i].marker != null ) {
            this.datasets[i].marker.setAttribute( "id", this.params.svgid + "-dataset" + i + "marker" );
            defs.appendChild( this.datasets[i].marker );
        }
    }
    // I will add a clipping path to this later
    
    // Styles
    
    var style = document.createElementNS( ns, "style" );
    svg.appendChild( style );

    var styletext = ".svgplotborderfill { stroke: none; fill: " + this.params.borderfill + "; }\n";

    styletext += ".svgplotborderstroke { fill: none; stroke: " + this.params.bordercolor +
        "; stroke-width: " + this.params.borderwid;
    if ( this.params.strokedash != null )
        styletext += "; stroke-dasharray: " + this.params.borderdash.join();
    styletext += "; }\n";
    
    styletext += ".svgplotaxes { stroke: " + this.params.axescolor + "; stroke-width: " + this.params.axeswid;
    if ( this.params.axesdash )
        styletext += "; stroke-dasharray: " + this.params.axesdash.join();
    styletext += "; fill: none }\n";

    styletext += ".svgplottick { stroke: " + this.params.tickcolor + "; stroke-width: " 
        + this.params.tickwid + "; }\n";
    styletext += ".svgplotsubtick { stroke: " + this.params.subtickcolor + "; stroke-width: "
        + this.params.subtickwid + "; }\n";
    styletext += ".svgplotticklabel { font-family: " + this.params.axislabelfamily + "; ";
    styletext += "font-size: " + this.params.axislabelsize + "px; font-weight: " + this.params.axislabelweight;
    styletext += "; font-style: " + this.params.axislabelstyle + "; }\n";

    styletext += ".svgplotaxislabel { font-family: " + this.params.axistitlefamily + "; ";
    styletext += "font-size: " + this.params.axistitlesize + "px; font-weight: " + this.params.axistitleweight;
    styletext += "; font-style: " + this.params.axistitlestyle + "; }\n";

    styletext += ".svgplottitle { font-family: " + this.params.titlefamily + "; ";
    styletext += "font-size: " + this.params.titlesize + "px; font-weight: " + this.params.titleweight;
    styletext += "; font-style: " + this.params.titlestyle + " }\n";
    
    styletext += ".svgplotgrid { stroke: " + this.params.gridcolor + "; stroke-width: "
        + this.params.gridwid + "; }\n";
    
    for ( var i in this.datasets ) {
        styletext += ".dataset" + i + " { stroke:" + this.datasets[i].color + "; ";
        styletext += "stroke-width: " + this.datasets[i].linewid + "; ";
        if ( this.datasets[i].dash != null )
            styletext += "; stroke-dasharray: " + this.datasets[i].dash.join() + "; ";
        styletext += "fill: none; }\n";

        styletext += ".errorbar" + i + " { stroke: " + this.datasets[i].color + "; ";
        styletext += "stroke-width: " + this.datasets[i].errbarwid + "; ";
        styletext += "fill: none; }\n";
    }

    style.appendChild( document.createTextNode( styletext ) );

    // Create the axis titles and labels, as we're going to need them
    // later.  Also add them to the svg so that we can get their size
    // with getBBox(); we'll move them to the right place later.

    var title = null;
    var titlewidth = 0;
    var titleheight = 0;
    if ( this.params.title != null ) {
        title = document.createElementNS( ns, "text" );
        title.setAttribute( "class", "svgplottitle" );
        title.appendChild( document.createTextNode( this.params.title ) );
        svg.appendChild( title );
        let size = this.elemSize( title );
        titlewidth = size.width;
        titleheight = size.height;
    }
    var xtitle = null;
    var xtitlewidth = 0;
    var xtitleheight = 0;
    if ( this.params.xtitle != null ) {
        xtitle = document.createElementNS( ns, "text" );
        xtitle.setAttribute( "class", "svgplotaxislabel" );
        xtitle.appendChild( document.createTextNode( this.params.xtitle ) );
        svg.appendChild( xtitle );
        let size = this.elemSize( xtitle );
        xtitlewidth = size.width;
        xtitleheight = size.height;
    }
    var ytitle = null;
    var ytitlewidth = 0;
    var ytitleheight = 0;
    if ( this.params.ytitle != null ) {
        ytitle = document.createElementNS( ns, "text" );
        ytitle.setAttribute( "class", "svgplotaxislabel" );
        ytitle.appendChild( document.createTextNode( this.params.ytitle ) );
        svg.appendChild( ytitle );
        let size = this.elemSize( ytitle );
        ytitlewidth = size.width;
        ytitleheight = size.height;
    }

    var retval = SVGPlot.generateTickValues( this._minx, this._maxx, this._xtickspacing );
    var xlabels = retval.ticks;
    var xsubticks = retval.nsubticks;
    // console.log( "x subticks: " + xsubticks + ", x labels: " + xlabels );
    var xlabelems = [];
    for ( var i in xlabels ) {
        let text = document.createElementNS( ns, "text" );
        text.setAttribute( "class", "svgplotticklabel" );
        text.appendChild( document.createTextNode( xlabels[i] ) );
        svg.appendChild( text );
        xlabelems.push( text );
    }
    let size = this.elemSize( xlabelems[0] );
    var xlabheight = size.height;
    // console.log( "xlabheight = " + xlabheight );
    
    var retval = SVGPlot.generateTickValues( this._miny, this._maxy, this._ytickspacing );
    var ylabels = retval.ticks;
    var ysubticks = retval.nsubticks;
    // console.log( "y subticks: " + ysubticks + ", y labels: " + ylabels );
    var ylabelems = [];
    var ylabwid = 0;
    for ( var i in ylabels ) {
        let text = document.createElementNS( ns, "text" );
        text.setAttribute( "class", "svgplotticklabel" );
        text.appendChild( document.createTextNode( ylabels[i] ) );
        svg.appendChild( text );
        let size = this.elemSize( text );
        if ( size.width > ylabwid ) ylabwid = size.width;
        ylabelems.push( text ); 
    }
    
    // Surrounding box
    
    var leftedge = this.params.pagemargin + 1.2*ytitleheight + 1.2*ylabwid;
    if ( this.params.left != null )
        leftedge = this.params.left;
    var rightedge = this.params.width - this.params.pagemargin;
    if ( this.params.right != null )
        rightedge = this.params.width - this.params.right
    var bottomedge = this.params.height - this.params.pagemargin - 1.2*xtitleheight - 1.2*xlabheight;
    if ( this.params.bottom != null )
        bottomedge = this.params.height - this.params.bottom
    var topedge = this.params.pagemargin + 1.1*titleheight;
    if ( this.params.top != null )
        topedge = this.params.top;
    
    var plotwidth = rightedge - leftedge;
    var plotheight = bottomedge - topedge;

    // Now that we know how big the thign is on the screen, we have to
    // re-evaluate plot min and max *again* in the case of equalaspect.
    // This potentially means re-generating tick labels!  Bah!

    if ( this.params.equalaspect )
    {
        if ( ( plotwidth / Math.abs( this._maxx - this._minx ) ) >
             ( plotheight / Math.abs(this._maxy - this._miny ) ) ) {
            let fullrange = ( plotwidth / plotheight ) * ( Math.abs(this._maxy - this._miny) );
            let currange = Math.abs( this._maxx - this._minx );
            let expamt = ( fullrange - currange ) / 2;
            if ( this._maxx > this._minx ) {
                this._maxx += expamt;
                this._minx -= expamt;
            }
            else {
                this._maxx -= expamt;
                this._minx += expamt;
            }
            for ( let yank of xlabelems ) yank.remove();
            xlabelems = [];
            retval = SVGPlot.generateTickValues( this._minx, this._maxx, this._xtickspacing );
            xlabels = retval.ticks;
            xsubticks = retval.nsubticks;
            for ( let i in xlabels ) {
                let text = document.createElementNS( ns, "text" )
                text.setAttribute( "class", "svgplotticklabel" );
                text.appendChild( document.createTextNode( xlabels[i] ) );
                svg.appendChild( text );
                xlabelems.push( text );
            }
        }
        else {
            let fullrange = ( plotheight / plotwidth ) * ( Math.abs(this._maxx - this._minx) );
            let currange = Math.abs( this._maxy - this._miny );
            let expamt = ( fullrange - currange ) / 2;
            if ( this._maxy > this._miny ) {
                this._maxy += expamt;
                this._miny -= expamt;
            }
            else {
                this._maxy -= expamt;
                this._miny += expamt;
            }
            ylabelems = [];
            retval = SVGPlot.generateTickValues( this._miny, this._maxy, this._ytickspacing );
            ylabels = retval.ticks;
            ysubticks = retval.nsubticks;
            for ( let i in ylabels ) {
                let text = document.createElementNS( ns, "text" )
                text.setAttribute( "class", "svgplotticklabel" );
                text.appendChild( document.createTextNode( ylabels[i] ) );
                svg.appendChild( text );
                ylabelems.push( text );
            }
        }
    }
    
    // Remember the plot limits we've figured out
    this.leftedge = leftedge;
    this.topedge = topedge;
    this.bottomedge = bottomedge;
    this.plotwidth = plotwidth;
    this.plotheight = plotheight;
    
    if ( this.params.borderfill != "none" ) {
        rect = document.createElementNS( ns, "rect" );
        rect.setAttribute( "class", "svgplotborderfill" );
        rect.setAttribute( "x", leftedge.toFixed( 2 ) );
        rect.setAttribute( "y", topedge.toFixed( 2 ) );
        rect.setAttribute( "width", plotwidth.toFixed( 2 ) );
        rect.setAttribute( "height", plotheight.toFixed( 2 ) );
        svg.appendChild( rect );
    }

    // Axis titles
    
    if ( title != null ) {
        title.setAttribute( "x", ( leftedge + plotwidth/2. - titlewidth/2. ).toFixed( 2 ) );
        title.setAttribute( "y", ( 1.05 * titleheight ) );
    }

    if ( xtitle != null ) {
        xtitle.setAttribute( "x", ( leftedge + plotwidth/2 - xtitlewidth/2 ).toFixed( 2 ) );
        xtitle.setAttribute( "y", ( this.params.height - 0.2*xtitleheight ).toFixed( 2 ) );
    }

    if ( ytitle != null ) {
        let yx = 1.1 * ytitleheight;
        let yy = this.params.pagemargin + plotheight/2 + ytitlewidth/2;
        ytitle.setAttribute( "x", yx.toFixed( 2 ) );
        ytitle.setAttribute( "y", yy.toFixed( 2 ) );
        ytitle.setAttribute( "transform", "rotate(270," + yx.toFixed( 2 ) + "," + yy.toFixed( 2 ) + ")" );
    }        

    // Grid, ticks and labels
    
    for ( var i = 0  ; i < xlabels.length ; ++i ) {
        let xdata = parseFloat( xlabels[i] );
        let x = ( xdata - this._minx ) * ( plotwidth / ( this._maxx-this._minx ) ) + leftedge;
        if ( ( this.params.gridwid > 0 ) && ( xdata > this._minx ) && ( xdata < this._maxx ) ) {
            let line = document.createElementNS( ns, "line" );
            line.setAttribute( "x1", x.toFixed(2) );
            line.setAttribute( "y1", bottomedge );
            line.setAttribute( "x2", x.toFixed( 2 ) );
            line.setAttribute( "y2", topedge );
            line.setAttribute( "class", "svgplotgrid" );
            svg.appendChild( line );
        }
        let line = document.createElementNS( ns, "line" );
        line.setAttribute( "x1", x.toFixed( 2 ) );
        line.setAttribute( "y1", ( bottomedge - this.params.ticklen/2 ).toFixed( 2 ) );
        line.setAttribute( "x2", x.toFixed( 2 ) );
        line.setAttribute( "y2", ( bottomedge + this.params.ticklen/2 ).toFixed( 2 ) );
        line.setAttribute( "class", "svgplottick" );
        svg.appendChild( line );
        let size = this.elemSize( xlabelems[i] );
        xlabelems[i].setAttribute( "x", ( x - size.width/2 ).toFixed( 2 ) );
        xlabelems[i].setAttribute( "y", ( bottomedge + 1.1 * xlabheight ).toFixed( 2 ) );
        svg.appendChild( xlabelems[i] );
        if ( i < xlabels.length-1 ) {
            for ( var j = 1 ; j < xsubticks ; ++j ) {
                let tickdatax = xdata + j * ( parseFloat( xlabels[i+1] ) - xdata ) / xsubticks;
                let tickx = ( tickdatax - this._minx ) * ( plotwidth / ( this._maxx-this._minx ) ) + leftedge;
                let line = document.createElementNS( ns, "line" );
                line.setAttribute( "x1", tickx.toFixed( 2 ) );
                line.setAttribute( "y1", ( bottomedge - this.params.subticklen/2 ).toFixed( 2 ) );
                line.setAttribute( "x2", tickx.toFixed( 2 ) );
                line.setAttribute( "y2", ( bottomedge + this.params.subticklen/2 ).toFixed( 2 ) );
                line.setAttribute( "class", "svgplotsubtick" );
                svg.appendChild( line );
            }
        }
    }

    for ( var i = 0 ; i < ylabels.length ; ++i ) {
        let ydata = parseFloat( ylabels[i] );
        let y = bottomedge - ( ydata - this._miny ) * ( plotheight / ( this._maxy-this._miny ) );
        if ( ( this.params.gridwid > 0 ) && ( ydata > this._miny ) && ( ydata < this._maxy ) ) {
            let line = document.createElementNS( ns, "line" );
            line.setAttribute( "x1", leftedge );
            line.setAttribute( "y1", y.toFixed( 2 ) );
            line.setAttribute( "x2", rightedge );
            line.setAttribute( "y2", y.toFixed( 2 ) );
            line.setAttribute( "class", "svgplotgrid" );
            svg.appendChild( line );
        }
        let line = document.createElementNS( ns, "line" );
        line.setAttribute( "x1", ( leftedge - this.params.ticklen/2 ).toFixed( 2 ) );
        line.setAttribute( "y1", y.toFixed( 2 ) );
        line.setAttribute( "x2", ( leftedge + this.params.ticklen/2 ).toFixed( 2 ) );
        line.setAttribute( "y2", y.toFixed( 2 ) );
        line.setAttribute( "class", "svgplottick" );
        svg.appendChild( line );
        let size = this.elemSize( ylabelems[i] );
        ylabelems[i].setAttribute( "x", ( leftedge - this.params.ticklen - size.width ).toFixed( 2 ) );
        // ROB... figuring out the relative-to-bbox-height distance is challenging...
        ylabelems[i].setAttribute( "y", ( y + size.height/3 ).toFixed( 2 ) );
        svg.appendChild( ylabelems[i] );
        if ( i < ylabels.length-1 ) {
            for ( var j = 1 ; j < ysubticks ; ++j ) {
                let tickdatay = ydata + j * ( parseFloat( ylabels[i+1] ) - ydata ) / ysubticks;
                let ticky = bottomedge - ( tickdatay - this._miny ) * ( plotheight / ( this._maxy-this._miny ) );
                let line = document.createElementNS( ns, "line" );
                line.setAttribute( "x1", ( leftedge - this.params.subticklen/2 ).toFixed( 2 ) );
                line.setAttribute( "y1", ticky.toFixed( 2 ) );
                line.setAttribute( "x2", ( leftedge + this.params.subticklen/2 ).toFixed( 2 ) );
                line.setAttribute( "y2", ticky.toFixed( 2 ) );
                line.setAttribute( "class", "svgplotsubtick" );
                svg.appendChild( line );
            }
        }
    }

    // Stroke border

    if ( this.params.borderwid > 0 && this.params.borderstroke != "none" ) {
        rect = document.createElementNS( ns, "rect" );
        rect.setAttribute( "class", "svgplotborderstroke" );
        rect.setAttribute( "x", leftedge.toFixed( 2 ) );
        rect.setAttribute( "y", topedge.toFixed( 2 ) );
        rect.setAttribute( "width", plotwidth.toFixed( 2 ) );
        rect.setAttribute( "height", plotheight.toFixed( 2 ) );
        svg.appendChild( rect );
    }

    // Axes

    // Add a clipping path to defs
    // ...this didn't work.  It would show nothing.  If I saved the generated source
    //  and reloaded it into firefox, it did work.  I really didn't understand.
    //  but, going with the inset() below seemed to work.  Scary.
    
    // var clippath = document.createElementNS( ns, "clipPath" );
    // clippath.setAttribute( "id", this.params.svgid + "-svgplotclip" );
    // // clippath.setAttribute( "clipPathUnits", "userSpaceOnUse" );
    // svg.appendChild( clippath );
    // var rect = document.createElement( "rect" );
    // rect.setAttribute( "x", leftedge.toFixed( 2 ) );
    // rect.setAttribute( "y", topedge.toFixed( 2 ) );
    // rect.setAttribute( "width", plotwidth.toFixed( 2 ) );
    // rect.setAttribute( "height", plotheight.toFixed( 2) );
    // clippath.appendChild( rect );
    // console.log( "Clipping path set to " + leftedge + " , " + topedge + " ... " + plotwidth + ", " + plotheight );
    
    // Datasets
    
    for ( var j in this.datasets ) {
        dataset = this.datasets[j];
        var points = "";
        for ( var i in dataset._x ) {
            var plotpt = this.dataToSVG( dataset._x[i], dataset._y[i], dataset );
            if ( i > 0 ) points += " ";
            points += Math.round(plotpt.x*100) / 100 + "," + Math.round(plotpt.y*100) / 100;
        }
        var polyline = document.createElementNS( ns, "polyline" );
        polyline.setAttribute( "class", "dataset" + j );
        polyline.setAttribute( "points", points )
        let clippath = "inset( "
            + topedge.toFixed(2) + " "
            + ( this.params.width - rightedge ).toFixed(2) + " "
            + ( this.params.height - bottomedge).toFixed(2) + " "
            + leftedge.toFixed(2) +") view-box" 
        polyline.setAttribute( "clip-path", clippath );
        if ( dataset.marker != null ) {
            polyline.setAttribute( "marker-start", "url(#" + this.params.svgid + "-dataset" + j + "marker)" );
            polyline.setAttribute( "marker-mid", "url(#" + this.params.svgid + "-dataset" + j + "marker)" );
            polyline.setAttribute( "marker-end", "url(#" + this.params.svgid + "-dataset" + j + "marker)" );
        }
        svg.appendChild( polyline );

        // Error bars
        if ( this.params.showerrbar ) {
            if ( dataset._dx.length > 0 ) {
                for ( var i in dataset._x ) {
                    let leftdx =  this.dataToSVG( dataset._x[i]-dataset._dx[i], dataset._y[i], dataset );
                    let rightdx = this.dataToSVG( dataset._x[i]+dataset._dx[i], dataset._y[i], dataset );
                    let line = document.createElementNS( ns, "line" );
                    line.setAttribute( "class", "errorbar" + j );
                    line.setAttribute( "x1", leftdx.x );
                    line.setAttribute( "y1", leftdx.y );
                    line.setAttribute( "x2", rightdx.x );
                    line.setAttribute( "y2", rightdx.y );
                    line.setAttribute( "clip-path", clippath );
                    svg.appendChild( line );
                }
            }
            if ( dataset._dy.length > 0 ) {
                for ( var i in dataset._x ) {
                    let botdy = this.dataToSVG( dataset._x[i], dataset._y[i]-dataset._dy[i], dataset );
                    let topdy = this.dataToSVG( dataset._x[i], dataset._y[i]+dataset._dy[i], dataset );
                    let line = document.createElementNS( ns, "line" );
                    line.setAttribute( "class", "errorbar" + j );
                    line.setAttribute( "x1", botdy.x );
                    line.setAttribute( "y1", botdy.y );
                    line.setAttribute( "x2", topdy.x );
                    line.setAttribute( "y2", topdy.y );
                    line.setAttribute( "clip-path", clippath );
                    svg.appendChild( line );
                }
            }
        }
    }

    // Notify listeners

    for ( let listener of self.redrawlisteners ) {
        listener( self );
    }

    // OMG that was huge
    
    return svg;
}

// **********************************************************************

SVGPlot.Plot.prototype.getSVG = function( width=800 )
{
    this.redraw( width );
    var svgtext = this.div.innerHTML;
    this.redraw();
    return svgtext;
}

// **********************************************************************

SVGPlot.Plot.prototype.getSVGElem = function( width=800 )
{
    this.redraw( width );
    var svg = this.svg.cloneNode( true );
    this.redraw();
    return svg;
}

// **********************************************************************

SVGPlot.Plot.prototype.screenToSVG = function( x, y, invtransmat=null )
{
    let screenpt = this.svg.createSVGPoint();
    screenpt.x = x;
    screenpt.y = y;
    if ( invtransmat == null )
        invtransmat = this.svg.getScreenCTM().inverse();
    var svgpt = screenpt.matrixTransform( invtransmat );
    return svgpt;
}

// **********************************************************************

SVGPlot.Plot.prototype.SVGToScreen = function( x, y, transmat=null )
{
    let svgpt = this.svg.createSVGPoint();
    svgpt.x = x;
    svgpt.y = y;
    if ( transmat == null )
        transmat = this.svg.getScreenCTM();
    var screenpt = svgpt.matrixTransform( transmat );
    return screenpt;
}

// **********************************************************************

SVGPlot.Plot.prototype.SVGToData = function( x, y, dataset=null )
{
    var point = {};
    point.x = ( x - this.leftedge ) * ( this._maxx - this._minx ) / this.plotwidth + this._minx;
    point.y = this._maxy - ( y - this.topedge ) * ( this._maxy - this._miny ) / this.plotheight;
    if ( dataset != null ) {
        point.x = ( point.x - dataset.offx ) / dataset.scalex;
        point.y = ( point.y - dataset.offy ) / dataset.scaley;
    }
    return point;
}

// **********************************************************************

SVGPlot.Plot.prototype.dataToSVG = function( x, y, dataset=null )
{
    if ( dataset != null ) {
        x = x * dataset.scalex + dataset.offx;
        y = y * dataset.scaley + dataset.offy;
    }
    var point = {};
    point.x = this.leftedge + ( x - this._minx ) * ( this.plotwidth / ( this._maxx - this._minx ) );
    point.y = this.topedge + ( this._maxy - y ) * ( this.plotheight / ( this._maxy - this._miny ) );
    return point;
}

// **********************************************************************

SVGPlot.Plot.prototype.screenToData = function( x, y, dataset=null, invtransmat=null )
{
    let pt = this.screenToSVG( x, y, invtransmat );
    return this.SVGToData( pt.x, pt.y, dataset );
}

// **********************************************************************

SVGPlot.Plot.prototype.dataToScreen = function( x, y, dataset=null, transmat=null )
{
    let pt = this.dataToSVG( x, y, dataset );
    return this.SVGToScreen( pt.x, pt.y, transmat );
}

// **********************************************************************

SVGPlot.Plot.prototype.click = function( event )
{
    if ( this.clicklisteners.length == 0 ) return;

    if ( event.altKey || event.ctrlKey || event.metaKey || event.shiftKey ) return;
    
    // let pt = this.svg.createSVGPoint();
    // pt.x = event.clientX;
    // pt.y = event.clientY;
    // var transmat = this.svg.getScreenCTM().inverse();
    // var invtransmat = transmat.inverse();
    // let transpt = pt.matrixTransform( transmat );
    // let datawid = this._maxx - this._minx;
    // let datahei = this._maxy - this._miny;
    // let clickdatax = ( ( transpt.x - this.leftedge ) * datawid / this.plotwidth + this._minx );
    // let clickdatay = this._maxy - ( ( transpt.y - this.topedge ) * datahei / this.plotheight );
    let clickpt = this.screenToData( event.clientX, event.clientY, null );
    
    // Find the closest point

    let minsetdex = null;
    let minpointdex = null;
    let mindist = null;
    let closex = null;
    let closey = null;
    for ( let setdex in this.datasets ) {
        let dataset = this.datasets[setdex];
        if ( dataset.clickselect ) {
            for ( let i in dataset._x ) {
                let x = dataset._x[i] * dataset.scalex + dataset.offx;
                let y = dataset._y[i] * dataset.scaley + dataset.offy;
                let dx = ( x - clickpt.x ) / ( this._maxx - this._minx );
                let dy = ( y - clickpt.y ) / ( this._maxy - this._miny );
                let dist = dx**2 + dy**2;
                if ( ( mindist == null ) || ( dist < mindist ) ) {
                    mindist = dist;
                    minpointdex = i;
                    minsetdex = setdex;
                    closex = x;
                    closey = y;
                }
            }
        }
    }

    // console.log( "Closest point is point " + minpointdex + " of dataset " + minsetdex + " at " +
    //              this.datasets[minsetdex]._x[minpointdex] + " , " + this.datasets[minsetdex]._y[minpointdex] );

    // let svgx = ( closex - this._minx ) * this.plotwidth / ( this._maxx - this._minx ) + this.leftedge;
    // let svgy = ( this._maxy - closey ) * this.plotheight / (this._maxy - this._miny ) + this.topedge;
    let svgpt = this.dataToSVG( closex, closey );
    
    if ( this.highlighter != null ) {
        this.highlighter.remove();
        this.highlighter = null;
    }
    this.highlighter = this.datasets[minsetdex].highlightSquare( svgpt.x, svgpt.y );
    this.svg.appendChild( this.highlighter );

    for ( let listener of this.clicklisteners ) {
        listener( { "setdex": minsetdex,
                    "pointdex": minpointdex,
                    "dataset": this.datasets[minsetdex],
                    "x": closex,
                    "y": closey } );
    }
}

// **********************************************************************

SVGPlot.Plot.prototype.mousedown = function( event )
{
    var self = this;
    const ns = "http://www.w3.org/2000/svg";

    if ( event.shiftKey ) {
        this.zooming = true;
        this.initmousex = event.clientX;
        this.initmousey = event.clientY;
        this.zoombox = document.createElementNS( ns, "rect" );
        this.zoombox.style["opacity"] = this.params.zoomboxopacity;
        this.zoombox.style["stroke"] = this.params.zoomboxborder;
        this.zoombox.style["fill"] = this.params.zoomboxcolor;
        this.zoombox.style["stroke-width"] = this.params.zoomboxborderwid;
        this.svg.appendChild( this.zoombox );
        this.svg.addEventListener( "mousemove", this.movecallback );
        this.svg.addEventListener( "mouseup", this.upcallback );
    }
}

SVGPlot.Plot.prototype.mousemoved = function( event )
{
    if ( this.zooming ) {
        this._autoscale = false;
        var pt = this.svg.createSVGPoint();
        pt.x = this.initmousex;
        pt.y = this.initmousey;
        var transmat = this.svg.getScreenCTM().inverse();
        this.zoominitpt = pt.matrixTransform( transmat );
        pt.x = event.clientX;
        pt.y = event.clientY;
        this.zoomfinalpt = pt.matrixTransform( transmat );
        var x, y, width, height;
        if ( this.zoominitpt.x < this.zoomfinalpt.x ) {
            x = this.zoominitpt.x;
            width = this.zoomfinalpt.x - x;
        } else {
            x = this.zoomfinalpt.x;
            width = this.zoominitpt.x - x;
        }
        if ( this.zoominitpt.y < this.zoomfinalpt.y ) {
            y = this.zoominitpt.y;
            height = this.zoomfinalpt.y - y;
        }
        else {
            y = this.zoomfinalpt.y;
            height = this.zoomfinalpt.y - y
        }
        this.zoombox.setAttribute( "x", x );
        this.zoombox.setAttribute( "y", y );
        this.zoombox.setAttribute( "width", width );
        this.zoombox.setAttribute( "height", height );
    }
}

SVGPlot.Plot.prototype.mouseup = function( event )
{
    this.svg.removeEventListener( "mousemove", this.movecallback );
    this.svg.removeEventListener( "mouseup", this.upcallback );

    if ( this.zooming )
    {
        var x0 = ( ( this.zoominitpt.x - this.leftedge )
                   * ( this._maxx - this._minx )
                   / this.plotwidth
                   + this._minx );
        var x1 = ( ( this.zoomfinalpt.x - this.leftedge )
                   * ( this._maxx - this._minx )
                   / this.plotwidth
                   + this._minx );
        var y0 = this._maxy - ( ( this.zoominitpt.y - this.topedge )
                                * ( this._maxy - this._miny )
                                / this.plotheight );
        var y1 = this._maxy - ( ( this.zoomfinalpt.y - this.topedge )
                                * ( this._maxy - this._miny )
                                / this.plotheight );
        if ( x0 < x1 ) {
            this._minx = x0;
            this._maxx = x1;
        }
        else {
            this._minx = x1;
            this._maxx = x0;
        }
        if ( y0 < y1 ) {
            this._miny = y0;
            this._maxy = y1;
        }
        else {
            this._miny = y1;
            this._maxy = y0;
        }
        this.zoombox.remove();
        this._zoommode = "manual";
        this.redraw();
    }
}



// **********************************************************************
// **********************************************************************
// **********************************************************************
// SVGPlot.Dataset encapsulates one data set
//
// Allowed values for marker:
//    dot  (filled circle)
//    circle (open circle)
//    square
//    filledsquare
//    diamond
//    filleddiamond
//    uptriangle
//    filleduptriangle
//    downtriangle
//    filleddowntriangle

SVGPlot.Dataset = function( inparams = {} )
{
    var params = { "name": null,
                   "x": [],
                   "y": [],
                   "dx": [],
                   "dy": [],
                   "pointnames": null,
                   "linewid": 4,
                   "errbarwid": 2,
                   "color": "#cc0000",
                   "highlightcolor": "#00cc00",
                   "dash": null,
                   "marker": "dot",
                   "markercolor": null,
                   "markersize": 12,
                   "markerstrokewid": 2,
                   "clickselect": true
                 };
    Object.assign( params, inparams );

    if ( params.markercolor == null ) params.markercolor = params.color;

    this.ordinal = 0;
    this.plot = null;
    this.name = params.name;
    this.clickselect = params.clickselect;
    this.highlightcolor = params.highlightcolor;
    this.markersize = params.markersize;
    this.linewid = params.linewid;
    this.errbarwid = params.errbarwid;
    this.color = params.color;
    this.dash = params.dash;
    if ( params.x.length != params.y.length ) {
        console.log( "ERROR: created a plot with different number of x and y. There will be further errors." );
        window.alert( "Plot creation error." );
        return;
    }
    this.replaceData( params.x, params.y, params.dx, params.dy, params.pointnames );
    
    this.marker = SVGPlot.Dataset.markerCode( params.marker, params.markercolor,
                                              params.markersize, params.markerstrokewid );

    this.scalex = 1.;
    this.offx = 0.;
    this.scaley = 1.;
    this.offy = 0.;
}

// **********************************************************************

SVGPlot.Dataset.prototype.replaceData = function( x, y, dx=[], dy=[], pointnames=null ) {
    this._x = [...x];
    this._y = [...y];
    this._dx = [...dx];
    this._dy = [...dy];
    if ( pointnames == null ) {
        this._pointnames = [];
        for ( var i in this._x.length ) {
            this._pointnames.push( "Point " + this.ordinal );
            this.ordinal += 1;
        }
    }
    else {
        if ( pointnames.length != this._x.length ) {
            console.log( "ERROR: wrong number of pointnames. There will be further errors." );
            window.alert( "Plot creation error." );
        }
        this._pointnames = [...pointnames];
    }
}

// **********************************************************************

SVGPlot.Dataset.markerCode = function( markername, markercolor, markersize, markerstrokewid )
{
    const ns = "http://www.w3.org/2000/svg";

    if ( markername == null ) return null;

    var marker = document.createElementNS( ns, "marker" );
    marker.setAttribute( "viewBox", "0 0 10 10" );
    marker.setAttribute( "refX", 5 );
    marker.setAttribute( "refY", 5 );
    marker.setAttribute( "markerWidth", markersize );
    marker.setAttribute( "markerHeight", markersize );
    marker.setAttribute( "stroke-width", markerstrokewid );
    marker.setAttribute( "markerUnits", "userSpaceOnUse" );
    if ( markername == "circle" || markername == "dot" ) {
        var circle = document.createElementNS( ns, "circle" );
        circle.setAttribute( "cx", 5 );
        circle.setAttribute( "cy", 5 );
        circle.setAttribute( "r", 5 );
        if ( markername == "dot" ) {
            circle.setAttribute( "fill", markercolor );
            circle.setAttribute( "stroke", "none" );
        }
        else {
            circle.setAttribute( "stroke", markercolor );
            circle.setAttribute( "fill", "none" );
        }
        marker.appendChild( circle );
    }
    else if ( markername == "square" || markername == "filledsquare" ) {
        var square = document.createElementNS( ns, "rect" );
        square.setAttribute( "x", 0 );
        square.setAttribute( "y", 0 );
        square.setAttribute( "width", 10 );
        square.setAttribute( "height", 10 );
        if ( markername == "filledsquare" ) {
            square.setAttribute( "fill", markercolor );
            square.setAttribute( "stroke", "none" );
        }
        else {
            square.setAttribute( "fill", "none" );
            square.setAttribute( "stroke", markercolor );
        }
        marker.appendChild( square );
    }
    else if ( markername == "diamond" || markername == "filleddiamond" ) {
        var diamond = document.createElementNS( ns, "polyline" );
        diamond.setAttribute( "points", "0,0 0,10 10,10 10,0 0,0" );
        if ( markername == "filleddiamond" ) {
            diamond.setAttribute( "fill", markercolor );
            diamond.setAttribute( "stroke", "none" );
        }
        else {
            diamond.setAttribute( "fill", "none" );
            diamond.setAttribute( "stroke", markercolor );
        }
        marker.appendChild( diamond );
    }
    else if ( markername == "uptriangle" || markername == "filleduptriangle" ) {
        var triangle = document.createElementNS( ns, "polyline" );
        triangle.setAttribute( "points", "0,0 5,10 10,0 0,0" );
        if ( markername == "filleduptriangle" ) {
            triangle.setAttribute( "fill", markercolor );
            triangle.setAttribute( "stroke", "none" );
        }
        else {
            triangle.setAttribute( "fill", "none" );
            triangle.setAttribute( "stroke", markercolor );
        }
        marker.appendChild( triangle );
    }
    else if ( markername == "downtriangle" || markername == "filleddowntriangle" ) {
        var triangle = document.createElementNS( ns, "polyline" );
        triangle.setAttribute( "points", "0,0 5,10 10,0 0,0" );
        if ( markername == "filleddowntriangle" ) {
            triangle.setAttribute( "fill", markercolor );
            triangle.setAttribute( "stroke", "none" );
        }
        else {
            triangle.setAttribute( "fill", "none" );
            triangle.setAttribute( "stroke", markercolor );
        }
        marker.appendChild( triangle );
    }
    else {
        console.log( "ERROR: unknown marker type " + markername );
        return null;
    }
        
    return marker;
}

// **********************************************************************

SVGPlot.Dataset.prototype.highlightSquare = function( svgx, svgy ) {
    const ns = "http://www.w3.org/2000/svg";

    var square = document.createElementNS( ns, "rect" );
    square.setAttribute( "x", svgx - 0.75*this.markersize );
    square.setAttribute( "y", svgy - 0.75*this.markersize );
    square.setAttribute( "width", 1.5*this.markersize );
    square.setAttribute( "height", 1.5*this.markersize );
    square.setAttribute( "fill", "none" );
    square.setAttribute( "stroke", this.highlightcolor );
    square.setAttribute( "stroke-width", 2 );
    return square;
}

// **********************************************************************

SVGPlot.Dataset.prototype.addPoint = function( x, y, dx=null, dy=null, name=null )
{
    if ( this._x.length == this._dx.length ) {
        if ( dx == null ) this._dx.push( 0. );
        else this._dx.push( dx );
    }
    else { 
        if ( dx != null ) {
            console.log( "WARNING: got a non-null dx for a dataset with no dx " +
                         "(I think... at least x and dx lengths don't match)" )
        }
    }
    if ( this._y.length == this._dy.length ) {
        if ( dy == null ) this._dy.push( 0. );
        else this._dy.push( dy );
    }
    else { 
        if ( dy != null ) {
            console.log( "WARNING: got a non-null dy for a dataset with no dy " +
                         "(I think... at least y and dy lengths don't match)" )
        }
    }
    this._x.push( x );
    this._y.push( y );
    if ( name == null )
        this._pointnames.push( "Point " + this.ordinal)
    else
        this._pointnames.push( name );
    this.ordinal += 1;
    if ( this.plot != null && this.plot._redrawonaddpoint )
        this.plot.redraw();
}

// **********************************************************************

SVGPlot.Dataset.prototype.removePoint = function( name ) {
    var dex = this._pointnames.indexOf( name );
    if ( dex < 0 ) {
        console.log( "WARNING: tried to remove point " + name + " from plot, but there is no such point." );
        return;
    }
    this._x.splice( dex, 1 );
    this._y.splice( dex, 1 );
    this._pointnames.splice( dex, 1 );
    if ( this._dx.length > 0 ) this._dx.splice( dex, 1 );
    if ( this._dy.length > 0 ) this._dy.splice( dex, 1 );
    if ( this.plot != null && this.plot._redrawonaddpoint )
        this.plot.redraw();
}

// **********************************************************************

export { SVGPlot };
