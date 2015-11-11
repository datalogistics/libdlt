for scene in LC80430332013243LGN00 LC80430342013243LGN00 LC80420352013236LGN00;
  do for ext in .jpg .zip .tar.gz _QB.png _TIR.jpg;
    do ./dispatch_file.py -f /data/GEC22_demo/${scene}${ext} -s ${scene} -d GEC22;
    done;
  done;
