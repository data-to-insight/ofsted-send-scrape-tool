# Ofsted-SEND-Scrape-Tool
On demand Ofsted SEND results summary via inspection reports scrape from the Ofsted.gov pages  
Published: https://data-to-insight.github.io/ofsted-send-scrape-tool/
-

### The inspection reports output summary is refreshed daily and timestamped for reference. 

## Brief overview
Note: The scrape only pulls those LA's that have a published report within the revised, post 2023 framework.
This project is based on a proof-of-concept, 'can we do this' basis. As such it's supplied very much with the disclaimer of please check the vitals if you're embedding it into something more critical, and likewise pls feel free to feedback into the project with suggestions. The structure of the code and processes have much scope for improvement, but some of the initial emphasis was on maintaining a level of readability so that others might have an easier time of taking it further. That said, we needed to take some of the scrape/cleaning processes further than anticipated due to inconsistencies in the source site/data and this has ultimately impacted the intended 're-usable mvp' approach to codifying a solution for the original problem. 

The results structure and returned data is based almost entirely on the originating SEND Summary produced/refreshed periodically by the ADCS; the use of which has previously underpinned several D2I projects. We're aware of several similar collections of longer-term work on and surrounding the Ofsted results theme, and would be happy to hear from those who perhaps also have bespoke ideas for changes here that would assist their own work. 

The scrape process is completed by running a single Python script: ofsted_send_scrape.py


## Export(s)
There are currently three exports from the script. 
### Results HTML page
Generated (as ./index.html) to display a refreshed subset of the SEND results summary. 

### Results Overview Summary
The complete SEND overview spreadsheet, exported to the git project root ./ as an .xlsx file for ease and also accessible via a download link from the generated results page (index.html). 

### All CSC inspections reports
During the scrape process, because we scan all the related CSC inspection pdf reports for each LA; these can be/are packaged up into tidy LA named folders (urn_LAname) within the git repo (./export_data/inspection_reports/). There is a lot of data here, but if you download the entire export_data folder after the script has run, with the overview summary sheet then the local_inspection_reports column active links will work and you can then easily access each LA's previous reports all in once place via the supplied hyperlink(s). *Note:* This is currently not an option when viewing the results on the web page/Git Pages.

## Known Bugs
Some LA's inspection reports have PDF encoding or inconsistent data in the published reports that is causing extraction issues & null data. 
We're working to address these, current known  issues are:
- The scrape only pulls those LA's that have a published report within the revised, post 2023 framework. We're considering whether/how best to include those inspections based both on previous frameworks and those with a written statement of action. 


## Imports(s)
There are currently two flat file(.csv) imports used. (/import_data/..)
### LA Lookup (/import_data/la_lookup/)
Allows us to add further LA related data including such as the historic LA codes still in use for some areas, but also enablers for further work, for example ONS region identifiers, and which CMS system LA's are using.
### Geospatial (/import_data/geospatial/)
This part of some ongoing work to access data we can use to enrich the Ofsted data with location based information, thus allowing us to visualise results on a map/choropleth. Some of the work towards this is completed, however because LA's geographical deliniations don't always map to ONS data, we're in the process of finding some work-arounds. The code and the reduced* GeoJSON data are there if anyone would like to fork the project and suggestion solutions. *GeoJSON data has been pre-processed to reduce the usually large file size and enable it within this repo/processing. 


## Future work

- Some of the in-progress efforts are included as a point of discuss or stepping stone for others to develop within the download .xlsx file. For example a set of columns detailing simplistic inspection sentiment analysis based on the language used in the most recent report (ref cols: sentiment_score, inspectors_median_sentiment_score, sentiment_summary, main_inspection_topics). *Note that the inclusion of these columns does not dictate that the scores are accurate, these additions are a starting point for discussion|suggestions and development!!*

- Geographical/Geospatial visualisations of results by region, la etc. are in progress. The basis for this is aready in place but some anomolies with how LA/counties boundary data is configured is an issue for some and thus the representation requires a bit more thought. 

- Improved automated workflow. We're currently still running the script manually until fixes can be applied to enable the Git Workflow(s) to run automatically/on a daily basis. We have the needed workflow scripts in place, but there is an ongoing issue in getting the py script to auto-run. Manual runs of the py script(+push/pull action) do correctly initiate the refresh of the html/GitPage.

- Provide active link access to all previous reports via the web front end. This currently only available when all post-script run files/folders are downloaded(this a v.large download if all LA folders included).

- Further development|bespoke work to improve potential tie-in with existing LA work that could use this tool or the resultant data. 


#### Contact via : datatoinsight.enquiries AT gmail.com


## Script admin notes
Simplified notes towards repo/script admin processes and enabling/instructions for non-admin running. 
### Script run intructions (User)
If looking to obtain a full instant refresh of the SEND output, the ofsted_childrens_services_inspection_scrape.PY should be run. These instructions for running in the cloud/Github. 
- Create a new Codespace (on main)
- Type run the following bash script at Terminal prompt to set up './setup.sh'
- Run the script (can right click script file and select 'run in python....'
- Download the now refreshed ofsted_childrens_services_inspection_scrape.XLSX (Right click, download)
- Close codespace (Github will auto-remove unused spaces later)
  
### Run notes (Admin)
If you experience a permissions error running the setup bash file. 

/workspaces/ofsted-send-scrape-tool (main) $ ./setup.sh
bash: ./setup.sh: Permission denied

then type the following, and try again: 
chmod +x setup.sh
