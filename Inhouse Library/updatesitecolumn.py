# -*- coding: utf-8 -*-
"""
Created on Tue Jun 02 10:20:04 2015

@author: chocolate server
"""

import updateLocalDbLib as updatedb

#update the site_column table
updatedb.updateSiteColumnTable()

#update the site_column_props table
updatedb.updateColumnPropsTable()

#update the site_rain_props table
updatedb.updateRainPropsTable()

#update the node_status table
updatedb.updateNodeStatusTable()