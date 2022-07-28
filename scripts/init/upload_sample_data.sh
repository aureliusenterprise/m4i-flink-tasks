#!/bin/bash

TOKEN=curl -d 'client_id=m4i_public' -d 'username=atlas' -d 'password=Ywc0aPJ3xw1LNFfS' -d 'grant_type=password' \
                'https://aureliusdev.westeurope.cloudapp.azure.com/anwo/auth/realms/m4i/protocol/openid-connect/token'

curl -g -v -X POST -H "Authorization: Bearer ${TOKEN:1:-1}" \
                -H "Content-Type: multipart/form-data" \
                -H "Cache-Control: no-cache" \
                -F data=@data/response1.zip \
                https://aureliusdev.westeurope.cloudapp.azure.com/anwo/atlas2/api/atlas/admin/import
