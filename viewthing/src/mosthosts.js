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
                            { "attributes": { "value": "list", "selected": "selected" },
                              "text": "List of Specific SNe" } )
        rkWebUtil.elemaker( "option", this.whichtoshow,
                            { "attributes": { "value": "many" },
                              "text": "All that satisfy...." } );
        rkWebUtil.elemaker( "br", this.frontpagediv );
        
        this.listdiv = rkWebUtil.elemaker( "div", this.frontpagediv );
        rkWebUtil.elemaker( "p", this.listdiv,
                            { "text": "Names of SNe to show (comma or space separated)" } );
        this.snlist = rkWebUtil.elemaker( "input", this.listdiv,
                                          { "attributes": { "type": "text",
                                                            "size": 80 } } );
        rkWebUtil.elemaker( "br", this.listdiv );
        rkWebUtil.button( this.listdiv, "show", function( e ) { self.showThings(); } );
        
        this.criteriondiv = rkWebUtil.elemaker( "div", null );
        let tab = rkWebUtil.elemaker( "table", this.criteriondiv, { "classes": [ "inputlayout" ] } );

        let tr = rkWebUtil.elemaker( "tr", tab );
        let td = rkWebUtil.elemaker( "td", tr );
        this.usenhosts = rkWebUtil.elemaker( "input", td,
                                             { "attributes": { "id": "criterion-usenhosts",
                                                               "type": "checkbox" } } );
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.elemaker( "label", td, { "text": "Min n. Hosts:",
                                           "attributes": { "for": "criterion-usenhosts" } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.minnhosts = rkWebUtil.elemaker( "input", td, { "attributes": { "type": "number",
                                                                            "min": 1,
                                                                            "max": 10,
                                                                            "value": 3,
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

// **********************************************************************

MostHosts.shower.prototype.render = function() {
    var self = this;
    
    rkWebUtil.wipeDiv( this.maindiv );

    let hbox = rkWebUtil.elemaker( "div", this.maindiv, { "classes": [ "hbox" ] } );

    let vbox = rkWebUtil.elemaker( "div", hbox, { "classes": [ "vbox", "borderbox" ] } );

    let p = rkWebUtil.elemaker( "p", vbox,
                                { "text": "Back to main page",
                                  "classes": [ "link" ],
                                  "click": (e) => { parent.render(); } } );

    let snlist = null;
    this.offset = null;
    this.numperpage = null;
    let minnhosts = null;
    
    if ( this.parent.whichtoshow.value == "list" ) {
        snlist = this.parent.snlist.value.strip().split( /[ ,]+/ );
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

    vbox = rkWebUtil.elemaker( "div", hbox, { "classes": [ "vbox" ] } );
    p = rkWebUtil.elemaker( "p", vbox, "Reject hosts if " );
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
    this.divs = {};
    this.svgs = {};
    this.imgs = {};
    this.hostcircles = {};
    
    var clipsize = 30;
    var imgscale = 4;

    var legacysurveydelay = 200;
    
    rkWebUtil.wipeDiv( this.paneldiv );

    this.totnumsne.innerHTML = data['ntotal'];
    if ( this.offset + this.numperpage > this.totnumsne )
        this.nextbutton.style.display = "none"
    
    for ( let sn of data.sne ) {
        this.sne.push( sn.snname );
        let div = rkWebUtil.elemaker( "div", this.paneldiv, { "classes": [ "snimg" ] } );
        let p =rkWebUtil.elemaker( "p", div, { "text": sn.snname } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.elemaker( "span", p, { "text": sn.hosts.length.toString() + " hosts" } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.elemaker( "span", p, { "text": sn.sn_ra.toString() + ", " + sn.sn_dec.toString(),
                                         "classes": [ "halfsize" ] } );
        let wrapperdiv = rkWebUtil.elemaker( "div", div, { "classes": [ "wrapper" ] } );
        let img = rkWebUtil.elemaker( "img", wrapperdiv, { "attributes":
                                                           { "src": "",
                                                             "alt": "Loading " + sn.snname } } );
        console.log( "Setting wize to " + ( imgscale * clipsize ).toString() + "px" );
        wrapperdiv.style.width = ( imgscale * clipsize ).toString() + "px";
        wrapperdiv.style.height = ( imgscale * clipsize ).toString() + "px";

        let svg = rkWebUtil.elemaker( "svg", wrapperdiv, { "svg": true } );
        // let svg = document.createElementNS("http://www.w3.org/2000/svg", "svg")
        // wrapperdiv.appendChild( svg );
        svg.setAttribute( "viewBox", "0 0 " + clipsize + " " + clipsize );
        rkWebUtil.elemaker( "line", svg,
                            { "attributes": { "x1": clipsize/2. - 1,
                                              "x2": clipsize/2. + 1,
                                              "y1": clipsize/2. + 1,
                                              "y2": clipsize/2. - 1 },
                              "classes": [ "sn" ],
                              "svg": true } );
        rkWebUtil.elemaker( "line", svg,
                            { "attributes": { "x1": clipsize/2. - 1,
                                              "x2": clipsize/2. + 1,
                                              "y1": clipsize/2. - 1,
                                              "y2": clipsize/2. + 1 },
                              "classes": [ "sn" ],
                              "svg": true } )

        this.hostcircles[ sn.snname ] = [];
        
        for ( let host of sn.hosts ) {
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
            let circle = rkWebUtil.elemaker( "circle", svg,
                                             { "attributes": { "cx": clipsize/2. - dra,
                                                               "cy": clipsize/2. - ddec,
                                                               "r": 1.5 },
                                               "classes": [ "goodhost" ],
                                               "svg": true } );
            this.hostcircles[ sn.snname ].push( circle );
        }


        this.imgs[ sn.snname ] = img;
        this.divs[ sn.snname ] = div;
        this.svgs[ sn.snname ] = svg;
    }
    this.updateHostColors();
    
    // To avoid sending too many requests too fast, spread them out

    let getimage = function( sndex ) {
        let sn = self.data.sne[ sndex ];
        console.log( "Doing " + sn.snname );
        self.imgs[sn.snname].setAttribute( "src",
                                           "https://www.legacysurvey.org/viewer/jpeg-cutout?"
                                           + "ra=" + sn.sn_ra + "&dec=" + sn.sn_dec
                                           + "&pixscale=1&width=" + clipsize + "&height=" + clipsize
                                           + "&layer=ls-dr9" );
        if ( sndex < self.sne.length-1 ) {
            setTimeout( () => { getimage( sndex + 1 ) }, legacysurveydelay );
        }
    }

    if ( this.sne.length > 0 ) getimage( 0 );
}
    
// **********************************************************************

MostHosts.shower.prototype.updateHostColors = function() {
    let anynotall = ( this.anyall.value == "any" );
    for ( let sn of this.data.sne ) {
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
            let circ = this.hostcircles[ sn.snname ][ hostnum ]
            circ.classList.remove( ...circ.classList );
            circ.classList.add( bad ? "badhost" : "goodhost" );
        }
    }
}

// **********************************************************************

export { MostHosts }
