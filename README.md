## ilab-eis-notebooks

Workflows by the Innovation Lab:
* Rechunker- Rechunks zarr files stored in s3 buckets 
* LIS Notebooks- Interactive data analysis and visualization of LIS data 
 
### S3 upload notes:

> aws s3 mv  <local_zarr_path>  <s3_zarr_path> --acl bucket-owner-full-control --recursive

For example:

> aws s3 mv  /home/jovyan/efs/data/rechunk/LIS.OL_1km.ROUTING.LIS_HIST.d01.zarr s3://eis-dh-hydro/rechunk/LIS/OL_1km/ROUTING/LIS_HIST.d01.zarr   --acl bucket-owner-full-control --recursive
