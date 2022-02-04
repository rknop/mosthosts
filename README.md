# Scripts for mosthosts

Libraries here include:

  * `mosthosts_desi.py` — for figuring out which Most Hosts objects have DESI observations
  * `desi_specinfo.py` — for actually getting the DESI spectrum of a Most Hosts host
  * `mosthosts_skyportal.py` — link to the DESI SkyPortal

## `mosthosts_desi.py` : Reading mosthosts and matching to DESI observations

This library ``mosthosts_desi.py`` has a class that reads the mosthosts
table into a Pandas dataframe, and then adds information for all of the
hosts from the zbest_daily and public_fibermap DESI tables.  Look at
that file for  documentation, or do:

```
    from mostshost_desi import MostHostsDesi
    help(MostHostsDesi)
````

The jupyter notebook ``specmatch.ipynb`` is an example of using this
library.

## `desi_specinfo.py` : Finding spectra

The library ``desi_specinfo.py`` is what you can use to pull out the
spectra of a given mosthosts object.  It will only work on NERSC,
because it assumes it has access to the DESI files that are on
cfs there.

This spectrum uses some of the standard DESI software, so will only work
if you've set up the DESI environment.  On Jupyter at NERSC, use the
"DESI master" kernel.  (Indeed, I should really be using the DESI
library better than I am right now.  Perhaps in a future version.)

To use it, import the file into your script.  Instantiate a SpectrumInfo
object with:

```
    specinfo = SpecInfo( ra, dec, desipasswd='...' )
```

where ``desipasswd`` must be the proper DESI password for connecting to
the databse.  ra and dec are the ra and dec where you want to find DESI
spectra.  By default, it will search the ``daily`` tables, but you can
pass ``collection='everest'`` if you want to search the everst release
instead.  (It doesn't support Denali.)  You can figure out a ra and dec
you want to use from the ``mosthost_desi.py`` library described above.

Once you've initialized the object, you can get a set of DESI target
IDs with:

```
    targetids = specinfo.targetids
```

For a single target ID, you can get the DESI spectra with:

```
    spectrum_list = specinfo.get_spectra( targetid )
```

Because there may well be multiple DESI spectra for a single targetid,
you get a list back.  Each element of the list is a
desi.spectrum.Spectra object.  You can extract the first spectrum from
the list with someting like:

   spectrum = spectrum_list[0]
   wavelength = spectrum.wave['brz']
   flux = spectrum.flux['brz'][0,:]
   dflux = numpy.sqrt( 1. / spectrum.ivarl['brz'][0,:] )

("ivar" is the "inverse variance" information in the
desi.spectrum.Spectra object).

By default, you get the spectra directly from the file.  You can pass a
parameter "smooth" to "get_spectra" to Gaussian smooth the spectrum.
The ivar field's meaning gets increasingly fraught as smooth gets
bigger.  (Multiple issues, one the biggest ones being that we should
really now have correlated errors between the pixels!)

You can also get some information about the spectrum by calling

```
    spectrum_info_list = specinfo.info_for_targetid( targetid )

You'll get a list back whose elements correspond to the elements of what
is returned by `get_spectra`; each element of the list is a dict with
fields:

  * z
  * zerr
  * zwarn
  * deltachi2
  * filename
  * tileid
  * petal_loc
  * device_loc
  * night

An example of its using SpectrumInfo is in ``desi_pullspec.ipynb``.
Note that to use that, you'll need to edit the second code block to put
in the right database password.  (I have it reading from a file in my
directories that isn't world readable, so the code won't run as is on
NERSC Jupyter for anybody else.)
