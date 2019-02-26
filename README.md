Architecture and work flow can be found at 
https://cseweb.ucsd.edu/~gmporter/classes/fa18/cse224/post/2018/11/08/hw-5-remote-procedure-calls-and-surfstore/

This page extends the basic MiniCloud to support customized block-placement algorithm
https://cseweb.ucsd.edu/~gmporter/classes/fa18/cse224/post/2018/11/09/hw-7-a-distributed-surfstore-service/

Below shows how to start the program. 

Modify config.txt to indicate the essential configurations.

Use the following commands to run the blockstore, metadata store and the client - 

1. Blockstore - 

   ```shell
   python3 blockstore.py 5000
   ```

2. Metadata store - 

   ```shell
   python3 metastore.py config.txt
   ```

3. Client - 

   ```shell
   // to download a file
   python3 client.py config.txt download myfile.jpg /
   
   // to upload a file
   python3 client.py config.txt upload myfile.jpg
   
   // to delete a file
   python3 client.py config.txt delete myfile.jpg
   ```
