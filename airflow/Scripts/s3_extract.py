# -*- coding: utf-8 -*-
"""S3 Extract.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/151VGRlo0rySLSdazynDg--ZzHaRMHjgD
"""

#upload kaggle json
from google.colab import files
files.upload()

os.environ['KAGGLE_CONFIG_DIR'] = "/content"

!mkdir -p ~/.kaggle
!cp kaggle.json ~/.kaggle/

!chmod 600 ~/.kaggle/kaggle.json

# Download all files as zip
!kaggle datasets download -d olistbr/brazilian-ecommerce
# Unzipping the downloaded dataset
!unzip brazilian-ecommerce.zip

!aws configure

# Upload zip file
!aws s3 cp brazilian-ecommerce.zip s3://ecommercelogistics/

#Unzip files
!aws s3 cp . s3://ecommercelogistics/unzip --recursive

!ls