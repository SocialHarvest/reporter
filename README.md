#Social Harvest (reporter)
[![Build Status](https://drone.io/github.com/SocialHarvest/reporter/status.png)](https://drone.io/github.com/SocialHarvest/reporter/latest) [![Coverage Status](https://coveralls.io/repos/SocialHarvest/reporter/badge.png?branch=master)](https://coveralls.io/r/SocialHarvest/reporter?branch=master)

http://www.socialharvest.io

Social Harvest is a scalable and flexible open-source social media analytics platform.

There are three parts to the platform. This harvester, a reporter API, and the [Social Harvest Dashboard](https://github.com/SocialHarvest/dashboard) 
for front-end visualizations and reporting through a web browser.

This application (reporter) returns harvested information from a database used by the harvester application by exposing a read-only API.

While Social Harvest&reg; is a registered trademark, this software is made publicly available under the GPLv3 license.
"Powered by Social Harvest&reg;" on any rendered web pages (ie. in the footer) and within any documentation, web sites, or other materials 
would very much be appreciated since this is an open-source project.

## Configuration

You'll need to create a JSON file for configuring Social Harvest. Ensure this configuration file is named ```social-harvest-conf.json``` 
and sits next to the binary Go built or next to the main.go file (unless you pass another location and name when running Social Harvest).

For an example configuration, see ```example-conf.json```

This application can use the same configuration file as the harvester application or a different one with less settings. You may, for example, 
want to keep various private keys out of the configuration for this application as it may be running on a different server than the harvester.

Note: If you are working with the Social Harvest Dashboard and are developing locally with ```grunt dev``` then you will likely be
running the dashboard on a Node.js server with a port of ```8881``` (by default) and you will need to configure CORS for that origin. 
You can add as many allowed origins as you like in the configuration.

