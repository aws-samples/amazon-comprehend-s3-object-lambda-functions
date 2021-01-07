This Readme contains instructions for amazon internal development of this package . 
##Do not publish this file to github


##Getting Started
1. Install pipenv
1. For building the package run following commands . You can look more targets in MakeFile
    1. `make clean`
    1. `make bootstrap`
    1. `make init`
    1. `make build`
1. In order to pick the latest API changes which are not present in public AWS SDK :   
    1. Checkout the package BotocoreDev and update the required files 
    1. install botocore package with   
`pipenv install botocore <path to locally checked out botocoredev package>` .  
 This would install botocore from in virtualenv of the current project from you local checked out package. Now your test cases should pass
    1. After `make build` is run, it will pull in all the dependencies in `.aws-sam/build` path . 
    You'd need to override the package in this build path with your local package .
    e.g `sudo cp -r ~/Workspace/S3Banner/src/BotocoreDev/botocore/ ./.aws-sam/build/PiiRedactionFunction/botocore/`
    2. Run the following command to build a deployment package now . Fill the appropriate values
    `sam package --s3-bucket $(PACKAGE_BUCKET) --output-template-file $(SAM_DIR)/packaged-template.yml` 
    3. Run the deploy command to deploy your Lambda
    `sam deploy --template-file .aws-sam/packaged-template.yml --stack-name my-stack --capabilities CAPABILITY_IAM`   
    
###  To local testing use sam local:
   
    `sam local invoke --event ./test/unit/data/event.json`  
or deploy it like this:
```
    sam deploy --template-file .aws-sam/packaged-template.yml --stack-name my-stack --capabilities CAPABILITY_IAM
```

To add dependencies add it in `[package]` section in Pipfile


       