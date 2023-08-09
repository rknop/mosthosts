INSTALLDIR = ../test_install/flask

INSTALL_HERE = server.py ap/__init__.py  ap/db.py ap/static/ap.css ap/static/ap.js ap/static/ap_start.js \
	ap/templates/base.html ap/templates/ap.html
INSTALL_PARENT = flaskauth.py
INSTALL_PARENT_STATIC = aes.js jsencrypt.min.js resetpasswd_start.js rkauth.js rkwebutil.js


install : $(patsubst %, $(INSTALLDIR)/%, $(INSTALL_HERE))
install : $(patsubst %, $(INSTALLDIR)/ap/%, $(INSTALL_PARENT))
install : $(patsubst %, $(INSTALLDIR)/ap/static/%, $(INSTALL_PARENT_STATIC))

$(patsubst %, $(INSTALLDIR)/%, $(INSTALL_HERE)): $(INSTALLDIR)/% : % | mkdirs
	cp -p $< $@

$(patsubst %, $(INSTALLDIR)/ap/%, $(INSTALL_PARENT)): $(INSTALLDIR)/ap/% : ../../%  | mkdirs
	cp -p $< $@

$(patsubst %, $(INSTALLDIR)/ap/static/%, $(INSTALL_PARENT_STATIC)): $(INSTALLDIR)/ap/static/% : ../../% | mkdirs
	cp -p $< $@

mkdirs:
	mkdir -p $(INSTALLDIR)
	mkdir -p $(INSTALLDIR)/ap
	mkdir -p $(INSTALLDIR)/ap/static
	mkdir -p $(INSTALLDIR)/ap/templates

clean:
	rm -vrf $(INSTALLDIR)/*
