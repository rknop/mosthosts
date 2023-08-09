INSTALLDIR = ../test_install/html

INSTALL_HERE = ap.css ap.js ap.py ap_start.js db.py
INSTALL_PARENT = aes.js auth.py config.py jsencrypt.min.js resetpasswd_start.js rkauth.js rkwebutil.js \
	svgplot.js svgplot.css testsvgplot.html

install : $(patsubst %, $(INSTALLDIR)/%, $(INSTALL_HERE) $(INSTALL_PARENT))

$(patsubst %, $(INSTALLDIR)/%, $(INSTALL_HERE)): $(INSTALLDIR)/% : % | $(INSTALLDIR)
	cp -p $< $@

$(patsubst %, $(INSTALLDIR)/%, $(INSTALL_PARENT)): $(INSTALLDIR)/% : ../../% | $(INSTALLDIR)
	cp -p $< $@

$(INSTALLDIR):
	mkdir -p $(INSTALLDIR)

clean:
	rm -vf $(INSTALLDIR)/*
