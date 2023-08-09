import { Mosthosts } from "./mosthosts.js"

Mosthosts.started = false;
Mosthosts.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !Mosthosts.started ) {
                Mosthosts.started = true;
                window.clearInterval( Mosthosts.init_interval );
                renderer = new MostHosts();
                renderer.init();
            }
        }
    },
    100);
