# Scripts for mosthosts

# Reading mosthosts and matching to DESI observations

The jupyter notebook ``specmatch.ipynb`` reads in the mosthosts table
and figures out what DESI observations (looking at the daily tables)
correspond to hosts in that table.  This script is what produces the csv
file of redshifts that I've sent out.  Look for the words **Look Here**;
at that point in the script, the ``mosthosts`` variable is a Pandas
dataframe that has lots of information in it.  The information is a bit
tangled, because each host might have multiple DESI observations.  As
such, the information about the DESI observations are in lists.

# Finding spectra

The library ``desi_specinfo.py`` is what you can use to pull out the
spectra of a given mosthosts object.  To use it, import the file into
your script.  Instantiate a SpectrumInfo object with:

```
    specinfo = SpecInfo( ra, dec, desipasswd='...' )
```

where ``desipasswd`` must be the proper DESI password for connecting
to the databse.  ra and dec are the ra and dec where you want to find
DESI spectra.  By default, it will search the ``daily`` tables, but you
can pass ``collection='everest'`` if you want to search the everst
release instead.  (It doesn't support Denali.)

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
