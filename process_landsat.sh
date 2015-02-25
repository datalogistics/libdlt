#!/bin/bash

SCENE=$1
URL=$2
FILE="scene.tar.gz"

time lors_download -t 10 -b 10m $URL -o $FILE

if [ ! -f $FILE ]; then
    echo "Could not download file!"
    exit
fi

mkdir -p temp

echo "Unpacking geoTIFF data for ${SCENE}..."
tar -vxf $FILE -C temp
echo "done"

cd temp

for BAND in 7 6 5 4 3 2 1; do
  gdalwarp -t_srs EPSG:3857 ${SCENE}_B${BAND}.TIF $BAND-projected.tif;
done

echo -n "Combining bands..."
convert -combine {4,3,2}-projected.tif RGB.tif &> /dev/null
convert -combine {5,6,4}-projected.tif NIR.tif &> /dev/null
convert -combine {7,5,1}-projected.tif SWIR.tif &> /dev/null
echo "done"

echo -n "RGB image..."
convert -channel R -gamma 1.0 -channel G -gamma 1.0 -channel B -gamma 1.0 \
	-channel RGB -sigmoidal-contrast 60x12% RGB.tif RGB-corrected.tif
echo "done"

echo -n "NIR image..."
convert -channel R -gamma 1.0 -channel G -gamma 1.0 -channel B -gamma 1.0 \
	-channel RGB -sigmoidal-contrast 25x18% NIR.tif NIR-corrected.tif
echo "done"

echo -n "SWIR image..."
convert -channel R -gamma 1.0 -channel G -gamma 1.0 -channel B -gamma 1.0 \
	-channel RGB -sigmoidal-contrast 25x18% SWIR.tif SWIR-corrected.tif
echo "done"

echo "Creating montage..."
montage -label %f -background '#336699' -geometry +4+4 -resize 600x600 \
	RGB-corrected.tif NIR-corrected.tif SWIR-corrected.tif \
	montage.tif

display montage.tif

echo -n "Cleaning up..."
cp montage.tif ../
cd ..
rm -rf temp
echo "script complete!!"
