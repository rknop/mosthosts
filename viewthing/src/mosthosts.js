import { rkAuth } from "./rkauth.js"
import { rkWebUtil } from "./rkwebutil.js"

var MostHosts = function() {
    this.maindiv = null;
    this.frontpagediv = null;
}

MostHosts.webapfullurl = window.location;
MostHosts.webapurl = window.location.pathname;

// **********************************************************************(

MostHosts.prototype.init = function() {
    this.maindiv = document.getElementById( "maindiv" );
    this.connector = new rkWebUtil.Connector( MostHosts.webapurl );
    this.render();
}

// **********************************************************************

MostHosts.prototype.render = function() {
    var self = this
    
    rkWebUtil.wipeDiv( this.maindiv );

    if ( this.frontpagediv == null ) {
        this.frontpagediv = rkWebUtil.elemaker( "div", null );

        this.whichtoshow = rkWebUtil.elemaker( "select", this.frontpagediv );
        rkWebUtil.elemaker( "option", this.whichtoshow, 
                            { "attributes": { "value": "list" },
                              "text": "List of Specific SNe" } )
        rkWebUtil.elemaker( "option", this.whichtoshow,
                            { "attributes": { "value": "many", "selected": "selected" },
                              "text": "All that satisfy...." } );
        rkWebUtil.elemaker( "br", this.frontpagediv );
        
        this.listdiv = rkWebUtil.elemaker( "div", null );
        rkWebUtil.elemaker( "p", this.listdiv,
                            { "text": "Names of SNe to show (comma or space separated)" } );
        this.snlist = rkWebUtil.elemaker( "input", this.listdiv,
                                          { "attributes": { "type": "text",
                                                            "size": 80 } } );
        rkWebUtil.elemaker( "br", this.listdiv );
        rkWebUtil.button( this.listdiv, "show", function( e ) { self.showThings(); } );
        
        this.criteriondiv = rkWebUtil.elemaker( "div", this.frontpagediv );
        let tab = rkWebUtil.elemaker( "table", this.criteriondiv, { "classes": [ "inputlayout" ] } );

        let tr = rkWebUtil.elemaker( "tr", tab );
        let td = rkWebUtil.elemaker( "td", tr );
        this.usenhosts = rkWebUtil.elemaker( "input", td,
                                             { "attributes": { "id": "criterion-usenhosts",
                                                               "type": "checkbox" } } );
        this.usenhosts.checked = true;
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.elemaker( "label", td, { "text": "Min n. Hosts:",
                                           "attributes": { "for": "criterion-usenhosts" } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.minnhosts = rkWebUtil.elemaker( "input", td, { "attributes": { "type": "number",
                                                                            "min": 1,
                                                                            "max": 10,
                                                                            "value": 5,
                                                                            "step": 1 } } );
        
        tr = rkWebUtil.elemaker( "tr", tab );
        td = rkWebUtil.elemaker( "td", tr );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Number per page:" } )
        td = rkWebUtil.elemaker( "td", tr );
        this.numperpage = rkWebUtil.elemaker( "input", td,
                                              { "attributes": { "type": "number",
                                                                "min": 1,
                                                                "max": 500,
                                                                "value": 20,
                                                                "step": 1 } } );

        tr = rkWebUtil.elemaker( "tr", tab );
        td = rkWebUtil.elemaker( "td", tr );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Starting Offset:" } )
        td = rkWebUtil.elemaker( "td", tr );
        this.pageoffset = rkWebUtil.elemaker( "input", td,
                                              { "attributes": { "type": "number",
                                                                "min": 0,
                                                                "value": 0,
                                                                "step": 1 } } );
        tr = rkWebUtil.elemaker( "tr", tab );
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.button( td, "Show", function( e ) { self.showThings(); } );

        this.whichtoshow.addEventListener(
            "change",
            function( e ) {
                if ( self.whichtoshow.value == "list" ) {
                    if ( self.frontpagediv.contains( self.criteriondiv ) )
                        self.frontpagediv.removeChild( self.criteriondiv );
                    self.frontpagediv.appendChild( self.listdiv );
                } else {
                    if ( self.frontpagediv.contains( self.listdiv ) )
                        self.frontpagediv.removeChild( self.listdiv );
                    self.frontpagediv.appendChild( self.criteriondiv );
                }
            }
        );

    }

    this.maindiv.appendChild( this.frontpagediv );
}

// **********************************************************************

MostHosts.prototype.showThings = function() {
    let shower = new MostHosts.shower( this );
    shower.render();
}

// **********************************************************************
// **********************************************************************
// **********************************************************************

MostHosts.shower = function( parent ) {
    this.parent = parent;
    // For convenience
    this.maindiv = parent.maindiv;
    this.connector = parent.connector;
}

MostHosts.shower.minclipsize = 15;
MostHosts.shower.maxclipsize = 240;

// **********************************************************************

MostHosts.shower.prototype.render = function() {
    var self = this;
    
    rkWebUtil.wipeDiv( this.maindiv );

    let hbox = rkWebUtil.elemaker( "div", this.maindiv, { "classes": [ "hbox" ] } );

    let vbox = rkWebUtil.elemaker( "div", hbox, { "classes": [ "vbox", "borderbox" ] } );

    let p = rkWebUtil.elemaker( "p", vbox,
                                { "text": "Back to main page",
                                  "classes": [ "link" ],
                                  "click": (e) => { self.parent.render(); } } );

    let snlist = null;
    this.offset = null;
    this.numperpage = null;
    let minnhosts = null;
    
    if ( this.parent.whichtoshow.value == "list" ) {
        snlist = this.parent.snlist.value.trim().split( /[ ,]+/ );
    }
    else {
        this.offset = this.parent.pageoffset.value;
        this.numperpage = this.parent.numperpage.value;
        if ( this.parent.usenhosts.checked ) {
            minnhosts = this.parent.minnhosts.value;
        }
    }

    this.totnumsne = rkWebUtil.elemaker( "span", null );
    if ( snlist != null ) {
        p = rkWebUtil.elemaker( "p", vbox, { "text": "Showing " + snlist.length + " of " } );
        p.appendChild( this.totnumsne );
        p.appendChild( document.createTextNode( " SNe" ) );
    }
    else {
        p = rkWebUtil.elemaker( "p", vbox, { "text": "Showing " + this.numperpage + " SNe ( of " } );
        p.appendChild( this.totnumsne );
        p.appendChild( document.createTextNode( " total) per page at offset " + this.offset ) );
    }
    p = rkWebUtil.elemaker( "p", vbox );
    this.nextbutton = null;
    this.prevbutton = null;
    if ( this.offset != null ) {
        if (this. offset > 0 )  {
            this.prevbutton = rkWebUtil.button( p, "Previous " + this.numperpage,
                                                function( e ) { window.alert( "Not implemented" ) } );
        }
        this.nextbutton = rkWebUtil.button( p, "Next " + this.numperpage,
                                            function( e ) { window.alert( "Not implemented" ) } );
    }

    vbox = rkWebUtil.elemaker( "div", hbox, { "classes": [ "vbox", "borderbox" ] } );
    p = rkWebUtil.elemaker( "p", vbox, { "text": "Reject hosts if " } );
    this.anyall = rkWebUtil.elemaker( "select", p,
                                      { "change": (e) => { self.updateHostColors() } } );
    rkWebUtil.elemaker( "option", this.anyall, { "attributes": { "value": "any", "selected": "selected" },
                                                 "text": "any" } );
    rkWebUtil.elemaker( "option", this.anyall, { "attributes": { "value": "all" }, "text": "all" } );
    p.appendChild( document.createTextNode( " of:" ) );

    let tab = rkWebUtil.elemaker( "table", vbox, { "classes": [ "inputlayout" ]} );
    
    this.use_fracflux = {};
    this.fracfluxltgt = {};
    this.fracflux = {};
    for ( let filt of [ 'g', 'r' ] ) {
        let tr = rkWebUtil.elemaker( "tr", tab );
        let td = rkWebUtil.elemaker( "td", tr );
        let use_fracflux = rkWebUtil.elemaker( "input", td, { "attributes": { "type": "checkbox",
                                                                              "id": "use-fracflux-" + filt },
                                                              "change": (e) => { self.updateHostColors() } } );
        rkWebUtil.elemaker( "label", td, { "attributes": { "for": "use-fracflux-" + filt }, "text": "use?" } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "fracflux_" + filt + "_dr9" } );
        td = rkWebUtil.elemaker( "td", tr );
        let fracfluxltgt = rkWebUtil.elemaker( "select", td, { "change": (e) => { self.updateHostColors() } } );
        rkWebUtil.elemaker( "option", fracfluxltgt, { "text": ">",
                                                      "attributes": { "value": ">",
                                                                      "selected": "selected" } } );
        rkWebUtil.elemaker( "option", fracfluxltgt, { "text": "<", "attributes": { "value": "<" } } );
        td = rkWebUtil.elemaker( "td", tr );
        let fracflux = rkWebUtil.elemaker( "input", td, { "attributes": { "type": "number", "value": "1.0" },
                                                           "change": (e) => { self.updateHostColors() } } );

        this.use_fracflux[filt] = use_fracflux;
        this.fracfluxltgt[filt] = fracfluxltgt;
        this.fracflux[filt] = fracflux;
    }

    this.sninfodiv = rkWebUtil.elemaker( "div", hbox, { "classes": [ "vbox", "borderbox" ] } );
    
    rkWebUtil.elemaker( "hr", this.maindiv );
    this.paneldiv = rkWebUtil.elemaker( "div", this.maindiv, { "text": "...loading..." } );

    this.connector.sendHttpRequest( "gethosts",
                                    { "snlist": snlist,
                                      "minnhosts": minnhosts,
                                      "numperpage": this.numperpage,
                                      "offset": this.offset },
                                    function( data ) { self.showImages( data ) }
                                  );
}

// **********************************************************************

MostHosts.shower.prototype.showImages = function( data ) {
    var self = this;

    this.data = data;
    this.sne = [];
    this.clipinfo = {};

    // this.divs = {};
    // this.svgs = {};
    // this.imgs = {};
    // this.hostcircles = {};
    
    var clipsize = 30;
    var imgscale = 4;
    var circler = 1.5;
    var strokewidth = 0.5;
    
    var legacysurveydelay = 200;
    
    rkWebUtil.wipeDiv( this.paneldiv );

    this.totnumsne.innerHTML = data['ntotal'];
    if ( this.offset + this.numperpage > this.totnumsne )
        this.nextbutton.style.display = "none"
    
    for ( let sn of data.sne ) {
        this.sne.push( sn.snname );
        let div = rkWebUtil.elemaker( "div", this.paneldiv,
                                      { "classes": [ "snimg" ] } );
        let p = rkWebUtil.elemaker( "p", div );
        rkWebUtil.elemaker( "span", p, { "text": sn.snname,
                                         "classes": [ "link" ],
                                         "click": (e) => { self.showSNInfo( sn ); } } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.elemaker( "span", p, { "text": sn.hosts.length.toString() + " hosts" } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.elemaker( "span", p, { "text": sn.sn_ra.toFixed(5) + ", " + sn.sn_dec.toFixed(5),
                                         "classes": [ "twothirdsize" ] } );
        let wrapperdiv = rkWebUtil.elemaker( "div", div, { "classes": [ "wrapper" ] } );
        let img = rkWebUtil.elemaker( "img", wrapperdiv, { "attributes": { "src": "",
                                                                           "alt": "Loading " + sn.snname },
                                                           "click": (e) => { self.showSNInfo( sn ); } } );
        wrapperdiv.style.width = ( imgscale * clipsize ).toString() + "px";
        wrapperdiv.style.height = ( imgscale * clipsize ).toString() + "px";

        let svg = rkWebUtil.elemaker( "svg", wrapperdiv, { "svg": true } );
        // let svg = document.createElementNS("http://www.w3.org/2000/svg", "svg")
        // wrapperdiv.appendChild( svg );
        svg.setAttribute( "viewBox", "0 0 " + clipsize + " " + clipsize );
        let slash1 = rkWebUtil.elemaker( "line", svg,
                                         { "attributes": { "x1": clipsize/2. - circler,
                                                           "x2": clipsize/2. + circler,
                                                           "y1": clipsize/2. + circler,
                                                           "y2": clipsize/2. - circler },
                                           "classes": [ "sn" ],
                                           "svg": true } );
        let slash2 = rkWebUtil.elemaker( "line", svg,
                                         { "attributes": { "x1": clipsize/2. - circler,
                                                           "x2": clipsize/2. + circler,
                                                           "y1": clipsize/2. - circler,
                                                           "y2": clipsize/2. + circler },
                                           "classes": [ "sn" ],
                                           "svg": true } )

        let hostcircles = [];
        
        for ( let host of sn.hosts ) {
            // Just make it; will move it to the right palce
            // in the call to positionXandOs
            let circle = rkWebUtil.elemaker( "circle", svg,
                                             { "attributes": { "cx": clipsize/2.,
                                                               "cy": clipsize/2.,
                                                               "r": circler },
                                               "classes": [ "goodhost" ],
                                               "svg": true } );
            hostcircles.push( circle );
        }

        let buttondiv = rkWebUtil.elemaker( "div", div, { "classes": [ "hbox" ] } );
        let zoominbutton = rkWebUtil.button( buttondiv, " + ",
                                             (e) => { self.zoomIn( sn ) } );
        let zoomoutbutton = rkWebUtil.button( buttondiv, " − ",
                                              (e) => { self.zoomOut( sn ) } );

        let sizespan = rkWebUtil.elemaker( "span", buttondiv, { "classes": [ "threeqsize", "emmarginleft" ],
                                                                "text": clipsize.toString() + '"' } )
        
        this.clipinfo[ sn.snname ] = { 'img': img,
                                       'div': div,
                                       'svg': svg,
                                       'zoominbutton': zoominbutton,
                                       'zoomoutbutton': zoomoutbutton,
                                       'sizespan': sizespan,
                                       'clipsize': clipsize,
                                       'imgscale': imgscale,
                                       'circler': circler,
                                       'strokewidth': strokewidth,
                                       'slash1': slash1,
                                       'slash2': slash2,
                                       'hostcircles': hostcircles
                                     };
        this.positionXandOs( this.clipinfo[ sn.snname ], sn );
    }
    this.updateHostColors();
    
    // To avoid sending too many requests too fast, spread them out

    let getimage = function( sndex ) {
        let sn = self.data.sne[ sndex ];
        let sninfo = self.clipinfo[ sn.snname ];
        sninfo.img.setAttribute( "src", ( "https://www.legacysurvey.org/viewer/jpeg-cutout?"
                                          + "ra=" + sn.sn_ra + "&dec=" + sn.sn_dec
                                          + "&pixscale=1&width=" + clipsize + "&height=" + clipsize
                                          + "&layer=ls-dr9" ) );
        if ( sndex < self.sne.length-1 ) {
            setTimeout( () => { getimage( sndex + 1 ) }, legacysurveydelay );
        }
    }

    if ( this.sne.length > 0 ) getimage( 0 );
}
    
// **********************************************************************

MostHosts.shower.prototype.showSNInfo = function( sn ) {
    rkWebUtil.wipeDiv( this.sninfodiv );

    let hbox = rkWebUtil.elemaker( "div", this.sninfodiv, { "classes": [ "hbox" ],
                                                            "text": sn.snname + "  (" +
                                                            sn.sn_ra.toFixed(5) + ", " +
                                                            sn.sn_dec.toFixed(5) + ")" } );
    let tab = rkWebUtil.elemaker( "table", this.sninfodiv, { "classes": [ "emmargintop" ] } );
    let tr = rkWebUtil.elemaker( "tr", tab );
    let th = rkWebUtil.elemaker( "th", tr, { "text": "Host #" } );
    th = rkWebUtil.elemaker( "th", tr, { "text": 'Δra (")' } );
    th = rkWebUtil.elemaker( "th", tr, { "text": 'Δdec (")' } );
    th = rkWebUtil.elemaker( "th", tr, { "text": "source" } );

    for ( let i in sn.hosts ) {
        let host = sn.hosts[i];
        tr = rkWebUtil.elemaker( "tr", tab );
        let td = rkWebUtil.elemaker( "td", tr, { "text": i } );
        let source = "dr9";
        let hostra = host.ra_dr9;
        let hostdec = host.dec_dr9;
        if ( ( hostra == null ) || ( hostra.length == 0 ) ) {
            source = "sga";
            hostra = host.ra_sga;
            hostdec = host.dec_sga;
        }
        let dra = ( ( hostra - sn.sn_ra ) * Math.cos( sn.sn_dec * Math.PI / 180. ) ) * 3600.;
        let ddec = ( hostdec - sn.sn_dec ) * 3600.;
            
        td = rkWebUtil.elemaker( "td", tr, { "text": dra.toFixed(1) } );
        td = rkWebUtil.elemaker( "td", tr, { "text": ddec.toFixed(1) } );
        td = rkWebUtil.elemaker( "td", tr, { "text": source } );
    }
}

// **********************************************************************

MostHosts.shower.prototype.updateHostColors = function() {
    let anynotall = ( this.anyall.value == "any" );
    for ( let sn of this.data.sne ) {
        let sninfo = this.clipinfo[ sn.snname ];
        for ( let hostnum in sn.hosts ) {
            let anygood = false;
            let allgood = true;
            for ( let filt in this.fracflux ) {
                if ( this.use_fracflux[filt].checked ) {
                    let cutoff = this.fracflux[filt].value;
                    let val = sn.hosts[hostnum]["fracflux_" + filt + "_dr9"]
                    if ( ( ( this.fracfluxltgt[filt].value == "<" ) && ( val < cutoff ) )
                         ||
                         ( ( this.fracfluxltgt[filt].value == ">" ) && ( val > cutoff ) )
                       ) {
                        allgood = false;
                    }
                    else {
                        anygood = true;
                    }

                }
            }
            let bad = ( anynotall && (!allgood) ) || ( (!anynotall) && (!anygood) );
            let circ = sninfo.hostcircles[ hostnum ]
            circ.classList.remove( ...circ.classList );
            circ.classList.add( bad ? "badhost" : "goodhost" );
        }
    }
}

// **********************************************************************

MostHosts.shower.prototype.positionXandOs = function( clipinfo, sn ) {
    clipinfo.slash1.setAttribute( "x1", clipinfo.clipsize/2. - clipinfo.circler );
    clipinfo.slash1.setAttribute( "x2", clipinfo.clipsize/2. + clipinfo.circler );
    clipinfo.slash1.setAttribute( "y1", clipinfo.clipsize/2. + clipinfo.circler );
    clipinfo.slash1.setAttribute( "y2", clipinfo.clipsize/2. - clipinfo.circler );
    clipinfo.slash2.setAttribute( "x1", clipinfo.clipsize/2. - clipinfo.circler );
    clipinfo.slash2.setAttribute( "x2", clipinfo.clipsize/2. + clipinfo.circler );
    clipinfo.slash2.setAttribute( "y1", clipinfo.clipsize/2. - clipinfo.circler );
    clipinfo.slash2.setAttribute( "y2", clipinfo.clipsize/2. + clipinfo.circler );

    clipinfo.slash1.style.strokeWidth = clipinfo.strokewidth.toString() + "px";
    clipinfo.slash2.style.strokeWidth = clipinfo.strokewidth.toString() + "px";
    
    for ( let i in sn.hosts ) {
        let host = sn.hosts[ i ];
        // The next 12 lines are directly
        //   copied from showImages;
        //   make a function.
        let ra = host.ra_dr9;
        let dec = host.dec_dr9;
        if ( ( ra == null ) || ( ra.length == 0 ) ) {
            ra = host.ra_sga;
            dec = host.dec_sga;
        }
        ra = parseFloat( ra );
        dec = parseFloat( dec );
        let snra = parseFloat( sn.sn_ra );
        let sndec = parseFloat( sn.sn_dec );
        let dra = ( ( ra - snra ) * Math.cos( dec * Math.PI / 180. ) ) * 3600.;
        let ddec = ( dec - sndec ) * 3600.;
        let circle = clipinfo.hostcircles[ i ];
        circle.setAttribute( "cx", clipinfo.clipsize/2. - dra );
        circle.setAttribute( "cy", clipinfo.clipsize/2. - ddec );
        circle.setAttribute( "r", clipinfo.circler );
        circle.style.strokeWidth = clipinfo.strokewidth.toString() + "px";
    }
}
    
// **********************************************************************

MostHosts.shower.prototype.zoomIn = function( sn ) {
    let clipinfo = this.clipinfo[ sn.snname ];

    if ( clipinfo.clipsize <= MostHosts.shower.minclipsize ) return;

    clipinfo.clipsize /= 2;
    clipinfo.imgscale *= clipinfo;
    clipinfo.circler /= 2;
    clipinfo.strokewidth /= 2;

    this.finishZoom( clipinfo, sn );
}

// **********************************************************************

MostHosts.shower.prototype.zoomOut = function( sn ) {
    let clipinfo = this.clipinfo[ sn.snname ];

    if ( clipinfo.clipsize >= MostHosts.shower.maxclipsize ) return;

    clipinfo.clipsize *= 2;
    clipinfo.imgscale /= 2;
    clipinfo.circler *= 2;
    clipinfo.strokewidth *= 2;
    
    this.finishZoom( clipinfo, sn );
}

// **********************************************************************

MostHosts.shower.prototype.finishZoom = function( clipinfo, sn ) {
    clipinfo.sizespan.innerHTML = clipinfo.clipsize.toString() + '"'
    clipinfo.img.setAttribute( "src", "" );
    clipinfo.svg.setAttribute( "viewBox", "0 0 " + clipinfo.clipsize + " " + clipinfo.clipsize );
    this.positionXandOs( clipinfo, sn );

    if ( clipinfo.clipsize <= MostHosts.shower.minclipsize )
        clipinfo.zoominbutton.disabled = true;
    else
        clipinfo.zoominbutton.disabled = false;

    if ( clipinfo.clipsize >= MostHosts.shower.maxclipsize )
        clipinfo.zoomoutbutton.disabled = true;
    else
        clipinfo.zoomoutbutton.disabled = false;

    clipinfo.img.setAttribute( "src", ( "https://www.legacysurvey.org/viewer/jpeg-cutout?"
                                        + "ra=" + sn.sn_ra + "&dec=" + sn.sn_dec
                                        + "&pixscale=1&width=" + clipinfo.clipsize + "&height=" + clipinfo.clipsize
                                        + "&layer=ls-dr9" ) );
}

// **********************************************************************

export { MostHosts }
