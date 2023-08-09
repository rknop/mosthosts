# rkwebutil

Several utilities useful for web applications (and maybe some other things):

* rkwebutil.js : a set of random javascript routines I use myself
* config.py : a python config system that reads yaml files which can override each other
* rkauth : an authentication system for webaps using web.py, SQLAlchmey, and Postgres

rkwebutil is (c) 2023 by Robert Knop, and is available under the GNU GPL v3 (see gpl-3.0.txt).

---

## config.py

A class that reads a YAML file and then provides access to the contents of that yaml hierarchy via period-separated key strings.  Has provisions for including other yaml files that will be pre- or post-loaded to seed or replace the config variables.  Documentation is in the python docstring for the `Config` class in `config.py`

---

## rkwebutil.js

Several random javascript utilties that I use all the time myself.

* `rkWebUtil.wipeDiv(div)` : remove all children of document element `div` (it doesn't actually have to be a div element)
* `rkWebUtil.elemaker(elemtype, parent, inprops)` : shorthand for creating a document element.  (MORE DOCUMENTATION NEEDED.)
* `rkWebUtil.button(parent, title, callback)` : shorthand for creating a button.  (MORE DOCUMENTATION NEEDED.)
* `rkWebUtil.popupMenu(items, callbacks, classes, title=null, titleclasses=[], hrclasses=[])` : todo
* `rkWebUtil.parseStandardDateString(datestring)` : create a `Date` object from a string of the form `yyyy-mm-dd hh:mm:ss`, assuming that the datestring is in UTC.
* `rkWebUtil.dateFormat(date)` : convert a `Date` to `yyyy-mm-dd hh:mm:ss` in the local time zone.
* `rkWebUtil.dateUTCFormat(date)` : convert a `Date` to `yyyy-mm-dd hh:mm:ss` in UTC.
* `rkWebUtil.validateWidgetDate(datestr)` : todo
* `rkWebUtil.hideOrShow(widget, parameter, hideparams, showparams, displaytype="block")` : todo
* `rkWebUtil.arrayBufferToB64(buffer)` : todo
* `rkWebUtil.colorIntToCSSColor(colint)` : takes an integer representing a color and makes a `#rrggbb` CSS hex string from it.  The integer assumes that r, g, and b are all in the range 0-255, and the integer is `r*(256^2)+g*256+b`.
* `rkWebUtil.Connector(app)` : a class for sending HTTP requests via XMLHttpRequst, interpreting a JSON response, and calling callbacks with the data from the JSON reponse.


---

## rkauth

A system for defining users and authenticating them.  Assumes use of SQLAlchmey, Postgres, and web.py.

Includes files:

* `auth.py`
* `rkauth.js`
* `resetpasswd_start.js`
* `aes.js`
* `jsencrypt.min.js`

See comments in `auth.py` for some inadequate documentation on what other python is needed in order to integrate this into a [web.py](https://webpy.org) application.

No user passwords are stored server-side, nor are transmitted during authentication.  The server stores a user's RSA public key and AES-encrypted private key.  When the user logs in, the server sends the encrypted private key and a challenge string (a randomly-generated v4 uuid) encrypted with the public key.  The javasript code client-side uses a user-entered password to decrypt the private key.  It then uses that private key to decrypt the challenge string, and sends that back to the server.  If the server receives the right challenge string, it considers the user authenticated.  This way, the user's password is only ever used client side.  If the server's database is compromised, user private keys cannot be recovered without breaking the encryption (or somehow determining the user's password).

When a user's password is reset, a RSA public and private key are created server-side and sent down to the user.  The client javascript code prompts the user for a password, which it uses to encrypt the private key with AES.  The encrypted private key is sent back to the server, which stores the public key and the encrypted private key.

Password resets are handled by creating a random link (a v4 uuid) that expires after 1 hour.  This link is sent to the email address that the database has for that user.  A user going to that link is able to reset their password.  This does mean, as with all systems that send password reset links through email, that user accounts can be hijacked by intercepting the email.

(Because the public and private RSA keys are created server side (using `pycryptodome`) and sent unencrypted to the client, it would be possible for somebody listening to the connection to grab the unencrypted private key.  This is the only time the private key is sent unencrypted; the server does not store it, and never again does the system send the private key unencrypted over the network.  An improvement might be to create the public and private key client side; that way, the private key would only ever be transmitted encrypted.  To bypass cracking the encryption, the attacker would need to inject code client-side to either grab the password or the encrypted private key.  But, of course, trojan horse code that comes in client-side can always sniff anything the user types, so that's not a vulnerability intrinsic to this system.  The problem is, with the libraries I'm using right now, it's easy to create the keys server-side in a manner that I can use them compatibly both server and client side, but I haven't found a javascript library that would create keys client-side that I could use with pycryptodome servers-side. )

### License info

* Uses `aes.js` from CryptoJS 3.1.2 by Jeff Mott, MIT license
* Uses `jsencrypt.min.js` by Kenji Urshima, MIT License (see `jsencrypt.min.js.LICENSE.txt`)
