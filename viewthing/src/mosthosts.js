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
        let tab = rkWebUtil.elemaker( "table", this.frontpagediv, { "classes": [ "inputlayout" ] } );
        let tr = rkWebUtil.elemaker( "tr", tab );

        let td = rkWebUtil.elemaker( "td", tr, { "text": "Number per page:" } )
        td = rkWebUtil.elemaker( "td", tr );
        this.numperpage = rkWebUtil.elemaker( "input", td,
                                              { "attributes": { "type": "number",
                                                                "min": 1,
                                                                "max": 500,
                                                                "value": 20,
                                                                "step": 1 } } );
        tr = rkWebUtil.elemaker( "tr", tab );
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
    
    let offset = this.parent.pageoffset.value;
    let numperpage = this.parent.numperpage.value;

    rkWebUtil.elemaker( "p", this.maindiv, { "text": "Showing " + numperpage +
                                             " SNe per page at offset " + offset  } );
    let p = rkWebUtil.elemaker( "p", this.maindiv );
    if ( offset > 0 ) {
        rkWebUtil.button( p, "Previous " + numperpage, function( e ) { window.alert( "Not implemented" ) } );
    }
    rkWebUtil.button( p, "Next " + numperpage, function( e ) { window.alert( "Not implemented" ) } );

    rkWebUtil.elemaker( "hr", this.maindiv );
    this.paneldiv = rkWebUtil.elemaker( "div", this.maindiv, { "text": "...loading..." } );
    
    this.connector.sendHttpRequest( "gethosts", { "numperpage": numperpage, "offset": offset },
                                    function( data ) { self.showImages( data ) } );
}

// **********************************************************************

MostHosts.shower.prototype.showImages = function( data ) {
    var self = this;

    this.data = data;
    this.sne = [];
    this.divs = {};
    this.svgs = {};
    this.imgs = {};

    var clipsize = 30;
    var imgscale = 4;

    var legacysurveydelay = 200;
    
    rkWebUtil.wipeDiv( this.paneldiv );
    
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
            rkWebUtil.elemaker( "circle", svg,
                                { "attributes": { "cx": clipsize/2. - dra,
                                                  "cy": clipsize/2. - ddec,
                                                  "r": 1.5 },
                                  "classes": [ "goodhost" ],
                                  "svg": true } );
        }


        this.imgs[ sn.snname ] = img;
        this.divs[ sn.snname ] = div;
        this.svgs[ sn.snname ] = svg;
    }

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

export { MostHosts }
