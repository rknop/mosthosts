import { MostHosts } from "./mosthosts.js"

MostHosts.started = false;
MostHosts.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !MostHosts.started ) {
                MostHosts.started = true;
                window.clearInterval( MostHosts.init_interval );
                renderer = new MostHosts();
                renderer.init();
            }
        }
    },
    100);
