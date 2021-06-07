# ilab-eis-notebooks
Scientific Notebooks for NASA's EIS project.


Conda Setup
---------------
Create your conda environment as follows:

    > conda create --name ilab_eis_notebooks 
    > conda activate ilab_eis_notebooks


 
### Upload

>> aws s3 mv  <local_zarr_path>  <s3_zarr_path> --acl bucket-owner-full-control --recursive

For example:

>> aws s3 mv  /home/jovyan/efs/data/rechunk/LIS.OL_1km.ROUTING.LIS_HIST.d01.zarr s3://eis-dh-hydro/rechunk/LIS/OL_1km/ROUTING/LIS_HIST.d01.zarr   --acl bucket-owner-full-control --recursive
