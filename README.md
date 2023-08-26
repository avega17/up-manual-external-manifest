## Manual workaround for generating external manifest file

### gclout cli configuration

After [installing the gcloud cli](https://cloud.google.com/sdk/docs/install) you can run the following command to configure the auth credentials to use google cloud storage:

```bash
gcloud auth application-default login
```

This will open a browser window and ask you to login to your google account. Once logged in, the terminal will display a message like this:

```bash
Credentials saved to file: [{home_folder}/.config/gcloud/application_default_credentials.json]
```

## Updating env variables

We are using [python-dotenv](https://github.com/theskumar/python-dotenv) to manage environment variables. To update the env variables, edit the `.env` file in the root of the project. Your file should look like this unless you are working on a different tenant:

```bash
GOOGLE_APPLICATION_CREDENTIALS={home_folder}/.config/gcloud/application_default_credentials.json
manifest_version=1.0.0
uis_version=3.0.3
env=dev
sov=rotw
tenant_name=exelon-comed
tenant_id=ccefc4d7-667c-5a5e-88cd-e19bfdb8e4c9
```

