# Recent Adjustments

### September 8, 2024
From June 1 in 2024, all the historical IMERG data has been revised to Version 07B along with the current IMERG data being generated daily. In the last patch we updated the link to download IMERG for recent and current data while the link for the past historic data was not updated to V07B. This was raising the 'Connection Reset' error if one tries to run RAT from say 2020 to 2024. The problem has been resolved in the [developer version](../../Development/DeveloperVersion/) of RAT. It has been released in the version of RAT [v3.0.14](../../Development/PatchNotes/#v3014). To [update](https://conda.io/projects/conda/en/latest/commands/update.html) RAT, please use `conda update rat` in your RAT environment. 

Due to the non-availability of IMERG data for April and May 2024 on the IMERG web server, users may encounter issues when running RAT for time periods that include these months. This issue is on the IMERG data provider's side, and we are hopeful it will be resolved soon.

### June 30, 2024
There is a change in the required permissions for Google Service Accounts to access Earth Engine API.  

Earlier users need to give one of the following roles to service accounts:  
    * Earth Engine Resource Viewer (roles/earthengine.viewer) OR  
    * Earth Engine Resource Writer (roles/earthengine.writer) OR  
    * Earth Engine Resource Admin (roles/earthengine.admin)  

Now, an additional role is required for service accounts to access Earth Engine. That role is as follows:  
    * Service Usage Consumer (roles/serviceusage.serviceUsageConsumer)

Additionally, now it is recommended that the google cloud project itself should to be registered for use with Earth Engine. Earlier instead of the cloud project, users had to register each of their service accounts to use with Earth Engine.  

We have updated the RAT documentation accordingly [here](../../QuickStart/GettingReady/#gee-credentials) in step 4 of task 3 and task 4A has been added as an alternative to task 4B for GEE Credentials.

### June 1, 2024
There is a change in the [version of IMERG data products](https://gpm.nasa.gov/data/news/imerg-v07b-early-and-late-run-begin-production) and therefore weblink to download precipitation data needs to be changed. The weblink has been updated in the [developer version](../../Development/DeveloperVersion/) of RAT. It has been released in the [v3.0.13](../../Development/PatchNotes/#v3013). To [update](https://conda.io/projects/conda/en/latest/commands/update.html) RAT, please use `conda update rat` in your RAT environment. 


