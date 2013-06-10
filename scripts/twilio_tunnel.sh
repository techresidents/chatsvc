#!/bin/bash

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` <tunnel_url>"
    echo "i.e. `basename $0` http://http://tsmz.localtunnel.me"
    exit 1
fi

BASEURL=$1
curl -X POST 'https://api.twilio.com/2010-04-01/Accounts/AC339bd35c385747713117ec475787c1f7/Applications/AP1672ddc9ec844ae9b0274a63262ccb4d.json' \
    -d "VoiceUrl=$BASEURL/chatsvc/twilio_voice" \
    -d "StatusCallback=$BASEURL/chatsvc/twilio_status" \
    -u AC339bd35c385747713117ec475787c1f7:38ecd9a4310677ac69b575cd8f724d31

