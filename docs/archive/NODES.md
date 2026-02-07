Raster Workflow

A. Copy data from blob to mounted azure file storage (this is step 1 for almost all workflows actually)

B. Validate Raster - run validation on user submitted geotiff to check CRS and other common TIFF errors- bit depth COG status we need to thoroughly examine these files; three potential outcomes:
B1. Reject - Tiff is corrupted and can't be fixed- job failure message to user blaming them for uploading shit data 
B2. Warn and fix - errors that can be automatically fixed (e.g. "TIFF has 64bit pixels will be downscaled to 16 or 32 if float is absolutely necessary","TIFF is missing CRS reference using parameter override")
B3. Green Light - TIFF is ready for processing
In the case of B2 or B3 the complete validation data needs to be in the database somewhere including band count statistics projection COG status etc etc

C. Does Tiling need to happen? if input geotiff is beyond a certain size (or if we calculate that outputs will be beyond a certain size) we make tiling schem and chop up outputs

D. Reproject GeoTIFF - we use 4236 by default, other CRS can be specified as the output but this feature will be hidden until future app releases

E. Cloud Optimize GeoTIFF (should D and E be one node? it could be the same cog_translate call- ideally one node that can do either or both...)

F. Update STAC metadata

If C. is true, D & E are parallelized for each tile and F has to do a different STAC pattern (tile collection with pgstac search registration vs just one .tif)