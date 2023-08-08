# RAT Secrets File 

As mentioned in the [requirements](../../QuickStart/UserGuide/#requirements), RAT requires three different set of credentials for IMERG, AVISO and Google Earth Engine. The user has to provide these credentials as mentioned below in a file with extension `.ini`. 
!!! tip_note "Tip"
    1. To get the credentials for these accounts, please follow the instructions [here](../../Preparation/GettingReady/).
    <br><br>
    2. A `secrets_template.ini` file is downloaded in 'params' directory inside project directory. It is recommended to create a copy of this file at a safe location, rename it to `secrets.ini` and fill in the credentials.

### Aviso

* <h6 class="parameter_heading">*`username:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Email address of the account used to register for AVISO product.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If account email used to register for AVISO product is 'xyz@gmail.com', then 
    ```
    [aviso]:
    username= xyz@gmail.com
    ```

* <h6 class="parameter_heading">*`pwd:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Password of the account, set by you, to access AVISO product.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If password of the account to access AVISO product is 'Pass@123', then 
    ```
    [aviso]:
    pwd= Pass@123
    ```

### Imerg

* <h6 class="parameter_heading">*`username:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Email address of the account used to register for IMERG product.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If account email used to register for IMERG product is 'xyz@gmail.com', then 
    ```
    [imerg]:
    username= xyz@gmail.com
    ```

* <h6 class="parameter_heading">*`pwd:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Password of the IMERG account to access IMERG product. 

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If password of the account to access IMERG product is 'Pass@123', then 
    ```
    [imerg]:
    pwd= Pass@123
    ```
    
### Ee

* <h6 class="parameter_heading">*`service_account:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Email address of the service account created to access earth engine api.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If email address of service account created is 'rat@globalrat.iam.gserviceaccount.com', then 
    ```
    [ee]:
    service_account= rat@globalrat.iam.gserviceaccount.com
    ```

* <h6 class="parameter_heading">*`key_file:`* :</h6> 
    <span class="requirement">Reuired parameter</span>

    <span class="parameter_property">Description </span>: Absolute path of the key file for the service account created in 'json' format.

    <span class="parameter_property">Default </span>: It is blank by default and can be filled by the user.

    <span class="parameter_property">Syntax </span>: If absolute path of the key file is '/cheetah/rat_project/confidential/earth_engine/globalrat-xxefebxxb773.json', then 
    ```
    [ee]:
    key_file= /cheetah/rat_project/confidential/earth_engine/globalrat-xxefebxxb773.json
    ```