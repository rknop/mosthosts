# Scripts for mosthosts

Libraries here include:

  * `mosthosts_desi.py` — for figuring out which Most Hosts objects have DESI targets and observations
  * `desi_specfinder.py` — for loading a DESI spectrum at user-specified RA/Dec; uses the DESI-standard desisepc library
  * `mosthosts_skyportal.py` — link to the DESI SkyPortal [CURRENTLY BROKEN]

## `mosthosts_desi.py` : Reading mosthosts and matching to DESI targets and observations

This library ``mosthosts_desi.py`` has a class that reads the mosthosts table into a Pandas dataframe, and then adds information for all of the hosts from the zbest_daily and public_fibermap DESI tables.  Look at that file for documentation, or do:

```
    from mostshost_desi import MostHostsDesi
    help(MostHostsDesi)
```

The jupyter notebook ``specmatch.ipynb`` is an example of using this library.

## `desi_specfinder.py` : Finding spectra at a given RA/Dec

For help, do:
```
    from desi_specfinder import SpectrumFinder
    help(SpectrumFinder)
```

Examples of use are in the jupyter noteboos `desi_spec_at_radec.ipynb` and `desi_pullspec.ipynb`.

# Other Things here

Some of this is random maintenance stuff I use:

  * `spectrum_uploader.py` — I use this to upload spectra to the DESI SkyPortal.  I haven't actually run this repository's file in a long time, so I'm not sure it works; Autmun Awbrey has been doing the SkyPortal spectrum uploading in recent months.
  * `mosthosts_source_info.py` — A hack script used to diagnose SkyPortal name mismatches (which still needs to be completed!)
  * `*.csv` — cached files written when I make a `MostHostsDesi` object (from `mosthosts_desi.py`).  These CSV files may be useful as a cache of information about MostHosts, but of course they're not necessarily going to be up to date.
  
