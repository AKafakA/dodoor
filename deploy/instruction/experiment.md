All of the testing scripts has been well packed in the `deploy/python/scripts/` directory with end-to-end shell script to trigger easily with 1-click.

After generate the host configuration and hostname list by following configuration-generation.md
To set up the testing cluster, just run the following command:
```bash
cd dodoor && sh deploy/script/setup.sh
```

And then run the scripts like 
```bash
sh deploy/script/end_to_end_exp/azure_vm_exp.sh
```
Different experiment script will be provided for different dataset and have tunable parameters list at head of the script.