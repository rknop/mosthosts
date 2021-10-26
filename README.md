# Scripts for mosthosts

## Reading mosthosts and matching to DESI observations

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

## Finding spectra

The library ``desi_specinfo.py`` is what you can use to pull out the
spectra of a given mosthosts object.  It will only work on NERSC,
because it assumes it has access to the DESI files that are on
cfs there.

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

Once you've initialized the object, you can get a list of DESI target
IDs with:

```
    targetids = specinfo.targetids()
```

For a single target ID, you can get the DESI spectra with:

```
    spectrum_list = specinfo.get_spectra( targetid )
```

Because there may well be multiple DESI spectra for a single targetid,
you get a list back.  Each element of the list is a dict with fields:

  * info
  * B_wavelength
  * B_flux
  * B_dflux
  * R_wavelength
  * R_flux
  * R_dflux
  * R_wavelength
  * R_dflux

By default, the fluxes are read in directly from the DESI files.  You
can pass a parameter ``smooth`` to ``get_spectra`` to Gaussian smooth
the spectrom.  (For example,
``spectrum_list=specinfo.get_spectra(targetid,smooth=2)``.)  The info
field of the dictionary has some information about the spectrum; it is
itself a dictionary with fields:

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
