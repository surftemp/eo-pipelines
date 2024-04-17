#!/bin/bash
N=$1
E=$2
FOLDER=$3
wget https://prism-dem-open.copernicus.eu/pd-desk-open-access/prismDownload/COP-DEM_GLO-30-DGED__2022_1/Copernicus_DSM_10_${N}_00_${E}_00.tar
tar xvf Copernicus_DSM_10_${N}_00_${E}_00.tar
cp Copernicus_DSM_10_${N}_00_${E}_00/DEM/Copernicus_DSM_10_${N}_00_${E}_00_DEM.tif $FOLDER
rm -f Copernicus_DSM_10_${N}_00_${E}_00.tar
rm -Rf Copernicus_DSM_10_${N}_00_${E}_00
