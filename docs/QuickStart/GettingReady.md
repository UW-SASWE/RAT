# Getting Ready

To run RAT successfully, it requires three different sets of credentials mentioned [here](../UserGuide/#requirements). In this section, detailed and stepwise instructions are provided to get the credentials required for executing RAT {{rat_version.major}}.{{rat_version.minor}}.

### AVISO Credentials
Follow the steps to set up AVISO account and its credentials which will be used to access and download JASON-3 altimetry data. 

<span class="preparation_step">Step 1:</span> <br>
Click [here](https://www.aviso.altimetry.fr/en/data/data-access/registration-form.html) to register for AVISO products.

<span class="preparation_step">Step 2:</span> <br>
Fill out the registration form shown below. <br> 
![Registration form for AVISO Products](../images/aviso/ss1.jpg)

<span class="preparation_step">Step 3:</span> <br>
Select ‘GDR/IGDR (Geophysical Data Records)’ in the Product Selection section as highlighted in the screenshot below. <br>
![Product Selection in Registration form for AVISO Products](../images/aviso/ss2.jpg)

<span class="preparation_step">Step 4:</span> <br>
Accept the terms and conditions.

<span class="preparation_step">Step 5:</span> <br>
Submit the form.

### IMERG Credentials
Follow the steps to set up IMERG account and its credentials which will be used to access and download [IMERG Late Precipitation product](https://gpm.nasa.gov/taxonomy/term/1415).

<span class="preparation_step">Step 1:</span> <br>
Click [here](https://registration.pps.eosdis.nasa.gov/registration/) to register for IMERG products.

<span class="preparation_step">Step 2:</span> <br>
Click on **Register** (circled in the screenshot below) to open the registration form. <br>
![Registration page for IMERG Product](../images/imerg/ss1.jpg)

<span class="preparation_step">Step 3:</span> <br>
Fill out the registration form shown below. Please make sure to check **'Near-Realtime Products'** (circled in the screenshot below).<br> 
![Registration form for IMERG Product](../images/imerg/ss2.jpg)

<span class="preparation_step">Step 4:</span> <br>
Click on Save. You will get a confirmation email and use that to complete the process.

### GEE Credentials
RAT requires [google service account](https://cloud.google.com/iam/docs/service-account-overview), which is associated with a cloud project and both the project and service account should have access to earth engine api. It is free to create with a google user account (gmail account). Complete the listed tasks by following the instructions to set up a GEE service account. 

<span class="preparation_task">Task 1 : Create a google cloud project</span> <br><br>
<span class="preparation_step">Step 1:</span> <br>
Click [here](https://developers.google.com/earth-engine/cloud/earthengine_cloud_project_setup) to create a google cloud project.<br>
<span class="preparation_step">Step 2:</span> <br>
Click on 'Create a Cloud project'.<br>
![Create cloud project page screenshot](../images/gee/ss1.jpg)
<span class="preparation_step">Step 3:</span> <br>
Enter a Project name and Click on ’CREATE’.<br>
![Enter project details page screenshot](../images/gee/ss2.jpg)

!!! tip_note "Tip"
    Project name can be something like ‘RAT-SE-Asia’ or ‘RAT Mekong’.

!!! note
    Please do not close the cloud project window which will open up as it will be used to continue Task-3. 

<span class="preparation_task">Task 2 : Enable Earth Engine API for the cloud project created.</span> <br><br>
<span class="preparation_step">Step 1:</span> <br>
Click [here](https://developers.google.com/earth-engine/cloud/earthengine_cloud_project_setup) to enable earth engine API.<br>
<span class="preparation_step">Step 2:</span> <br>
Click on 'Enable the Earth Engine API’.<br>
![Enable EE API page screenshot](../images/gee/ss3.jpg)
<span class="preparation_step">Step 3:</span> <br>
Make sure the right project is selected and Click on ’ENABLE’.
![Enabling EE API page screenshot](../images/gee/ss4.jpg)

<span class="preparation_task">Task 3 : Create a service account.</span> <br><br>
<span class="preparation_step">Step 1:</span> <br>
In the cloud project window opened up after completing task 1, make sure the right project is selected and Click on ‘IAM & Admin’ > ‘Service Accounts’.<br>
![Cloud project menu list screenshot](../images/gee/ss5.jpg)
<span class="preparation_step">Step 2:</span> <br>
Click on ‘CREATE SERVICE ACCOUNT’.<br>
![creating service account button screenshot](../images/gee/ss6.jpg)
<span class="preparation_step">Step 3:</span> <br>
Enter 'Service account name' and Click on ‘CREATE AND CONTINUE’.
![Service Account description screenshot](../images/gee/ss7.jpg)

!!! tip_note "Tip"
    1. Service account name can be something like ‘rat-Mekong-YOUR_NAME’.
    2. Description can be about who will be using this service account and for what.

<span class="preparation_step">Step 4:</span> <br>
Choose ‘Earth Engine’ > ‘Earth Engine Resource Admin’ for first role and choose ‘Service Usage’ > ‘Service Usage Consumer’ as another role.After selecting both roles, click on ‘CONTINUE’. And then click on ‘Done’.<br>
![Service Account role screenshot](../images/gee/ss8.jpg)
![Service Account role screenshot](../images/gee/ss14.jpg)
<span class="preparation_step">Step 5:</span> <br>
Click on ‘Actions’ > ‘Manage Keys’ for the service account you created.<br>
![managing service account key screenshot](../images/gee/ss9.jpg)
<span class="preparation_step">Step 6:</span> <br>
Click on ‘Add KEY’ > ‘Create new key’ and select json.<br>
![Adding service account key screenshot](../images/gee/ss10.jpg)

<span class="preparation_task">Task 4A (Recommended, in place of Task 4B) : Register the google cloud project to use Earth Engine.</span> <br><br>
<span class="preparation_step">Step 1:</span> <br>
Click [here](https://developers.google.com/earth-engine) to register the created google cloud project to use earth engine API and sign-in using the user ID used to create cloud project. <br>
<span class="preparation_step">Step 2:</span> <br>
Click on ‘Register for Earth Engine’ (and select your email address if not signed in already). Then click on 'Register a Noncommercial or Commercial Cloud project'.<br>
![Register google account EE screenshot](../images/gee/ss15.jpg)
![Register google account EE screenshot](../images/gee/ss16.jpg)
<span class="preparation_step">Step 3:</span> <br>
Select “Unpaid usage” if using for non-commercial activity. Select a suitable ‘Project type’ and click on ‘NEXT’.<br>
![Register google account EE screenshot](../images/gee/ss17.jpg)
<span class="preparation_step">Step 4:</span> <br>
Select “Choose an existing Google Cloud Project” and select the cloud project that you created in Task 1. Click on “Continue to Summary”.<br>
![Register google account EE screenshot](../images/gee/ss18.jpg)
<span class="preparation_step">Step 5:</span> <br>
Click on ‘Confirm’ and you can close the window.  
![Register google account EE screenshot](../images/gee/ss19.jpg)

<span class="preparation_task">Task 4B (If Task 4A not done) : Register the service account to use Earth Engine.</span> <br><br>
<span class="preparation_step">Step 1:</span> <br>
Click [here](https://developers.google.com/earth-engine/guides/service_account#register-the-service-account-to-use-earth-engine) to register the service account created to use earth engine API. <br>
<span class="preparation_step">Step 2:</span> <br>
Click on ‘this page’ and select your email address.<br>
![Register service account EE screenshot](../images/gee/ss11.jpg)
<span class="preparation_step">Step 3:</span> <br>
Fill out the form and accept the terms.<br>
![Registering user account EE form screenshot](../images/gee/ss12.jpg)
<span class="preparation_step">Step 4:</span> <br>
Click on submit. <br>
<span class="preparation_step">Step 5:</span> <br>
Again click [here](https://developers.google.com/earth-engine/guides/service_account#register-the-service-account-to-use-earth-engine) and repeat step 2 by clicking on ‘this page’. 
![Register service account EE screenshot](../images/gee/ss11.jpg)
<span class="preparation_step">Step 6:</span> <br>
Enter the created service account’s email address.<br>

!!! tip_note "Tip"
    Service account’s email address will look something like “foo-name@project-name.iam.gserviceaccount.com”

![Registering service account EE  screenshot](../images/gee/ss13.jpg)
<span class="preparation_step">Step 7:</span> <br>
Click on ‘REGISTER SERVICE ACCOUNT’. <br>

### RAT Credentials

RAT {{rat_version.major}}.{{rat_version.minor}} requires all the credentials created in the above sections in a single secret file. So, put all the credentials into a file named 'secrets.ini' by following the commands as mentioned [here in secrets file section](../Configuration/secrets.md).