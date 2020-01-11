#!/bin/bash
for year in {1930..1950}; do
# Create a directory for the year
    if [ ! -d "$year" ]; then
    echo Creating a directory.
        mkdir -p $year
    fi
# Download the data (if we don't already have it)
    if [ ! -f "$year/gsod_$year.tar" ]; then
    wget ftp://ftp.ncdc.noaa.gov/pub/data/gsod/$year/gsod_$year.tar -O $year/gsod_$year.tar
    fi

# Unzip the data
    if [ -f "$year/gsod_$year.tar" ]; then
    echo Unzipping the downloaded data.
        tar -xvf $year/gsod_$year.tar -C $year/
        rm $year/gsod_$year.tar
        for filename in `ls $year/*.gz`; do
            gunzip $filename
        done
    fi
done
for year in {1930..1950}; do
# Strip the first line of each of the .op files
    for filename in `ls $year/*.op`; do
        tail -n +2 $filename > $filename.header_stripped
        mv $filename.header_stripped $filename
    done
# Stack the .op files
    cat $year/*.op > $year.op
    rm -rf $year
done

#converting to csv
if [ -f "gsod.csv" ]; then
    rm gsod.csv
fi
for year in {1930..1950}; do
    echo $year
    awk '{printf "%s;%s;%s;%.1f;%.1f;%.1f;%.1f;%.1f;%.1f;%.1f;%.1f;%.1f;%.1f;%.2f;%.1f;%s;%s;%s;%s;%s;%s\n",$1,$2,$3,$4,$6,$8,$10,$12,$14,$16,$17,substr($0,103,6),substr($0,111,6),substr($0,119,5),$21,substr($22,1,1),substr($22,2,1),substr($22,3,1),substr($22,4,1),substr($22,5,1),substr($22,6,1)}' OFS=';' $year.op >> gsod2.csv;
    rm $year.op
done
