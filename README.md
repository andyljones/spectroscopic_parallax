### Notes 

  * While not strictly necessary, a lot of this was an experiment with EC2, where things like 'storing everything on S3' make more sense. 
  * Cross-matching needs [gaia_tools](https://github.com/jobovy/gaia_tools), which can be installed with `pip install git+git://github.com/jobovy/gaia_tools.git`.          
  * [APOGEE column definitions](https://data.sdss.org/datamodel/files/APOGEE_REDUX/APRED_VERS/APSTAR_VERS/ASPCAP_VERS/RESULTS_VERS/allStar.html)
  * [GAIA column definitions](https://gea.esac.esa.int/archive/documentation/GDR2/Gaia_archive/chap_datamodel/sec_dm_main_tables/ssec_dm_gaia_source.html)
  * [WISE column definitions](http://wise2.ipac.caltech.edu/docs/release/allwise/expsup/sec2_1a.html)
  * [WISE bulk downloads](https://irsa.ipac.caltech.edu/data/download/wise-allwise/)
  * [Paper](https://arxiv.org/pdf/1810.09468.pdf)
  * [GAIA Archive](https://gea.esac.esa.int/archive/)

### EC2
  * Use the [IAM](https://console.aws.amazon.com/iam/home) to create 
    * a user and an access key for that user, putting the AWS access key and secret in [`~/.aws/credentials`](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#iam-role)
    * an IAM role for EC2 allowing full access to S3, putting the IAM profile ARN in the `config.json`
  * Use the [EC2 dashboard](https://console.aws.amazon.com/ec2) to create 
    * an SSH key pair, putting the `.pem` file in `~/.ssh` and the key name in the `config.json`
    * a security group allowing SSH access, putting the group name in `config.json`
    * a security group allowing mutual access, putting the group name in `config.json`