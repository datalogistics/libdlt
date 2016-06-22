# =============================================================================
#  Data Logistics Toolkit (dlt-tools)
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
for scene in LC80430332013243LGN00 LC80430342013243LGN00 LC80420352013236LGN00;
  do for ext in .jpg .zip .tar.gz _QB.png _TIR.jpg;
    do ./dispatch_file.py -f /data/GEC22_demo/${scene}${ext} -s ${scene} -d GEC22;
    done;
  done;
