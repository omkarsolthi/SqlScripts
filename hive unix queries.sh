#!/bin/bash
tdate=`date +"%m-%d-%Y"`
ydate=`date +"%Y%m%d" -d "-2 day"`
echo "$ydate"

sleep 5


hive -e "select */ from logoud.ims limit 5" > teste.xls;
rs=$?

        if [ "$rs" = "0" ]; then
echo"job success"
        else
echo "Job Failed." | mail -s "Job Fail Notification" ogoud@walmartlabs.com
fi

echo "Mail Sent"

****************
if [ "$?" -ne "0" ] or use this ( var1=$? if [ "$var1" = "0" ] )
then
    <send me email however you want to about failure>
else
    <send me email about success how ever you want> 
fi

****************
The subject of the email can be specified with a -s and a list of address with -t. You can write the text on your own with the echo command:

echo "This will go into the body of the mail." | mail -s "Hello world" you@youremail.com
or get it from other files too:

mail -s "Hello world" you@youremailid.com < /home/calvin/application.log
mail doesn't support the sending of attachments, but Mutt does:

echo "Sending an attachment." | mutt -a file.zip -s "attachment" target@email.com
Note that Mutt's much more complete than mail. You can find better explanation here

************************
UPDATE
  table_name
SET
  column = 'value';
That will only "insert" data into a single column while leaving everything else undisturbed.