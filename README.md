# CSE 224 Homework 5

Basic starter code for Homework 5

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
