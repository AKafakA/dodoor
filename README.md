Dodoor prototype which including Dodoor scheduler and random/prequal and standard power of two and late binding schedulers.

The evaluation data is downloaded from Azure which is located at https://github.com/Azure/AzurePublicDataset/blob/master/AzureTracesForPacking2020.md and then run the scripts to extrac and generate data to target directory

```setup
python3 deploy/python/scripts/generate_data.py
```

To the experiments at private clusters, please take a look at this example config located at: /deploy/resources/configuration/example_dodoor_configuration.conf and the detailed configuration defination is located at src/main/java/edu/cam/dodoor/DodoorConf.java

A bash script to conduct the end to end example is provided at /deploy/script/single_test_caelum.sh. It is also required to replace the hostname and host ip applied defined under deploy/resources/host_addresses/caelum/small_caelum and deploy/resources/host_addresses/caelum.

And  /deploy/script/single_test_cloudlab.sh is provided for large scale experiments on cloudlab shows in the paper. It is required to pull the cl_manifest.xml files after the cluster successfully provision and then run the 

```setup
python3 deploy/python/scripts/cl_manifest_parse.py
```
to set up the testing connections from local and then either /deploy/script/single_test_cloudlab.sh or /deploy/script/multi_test.sh from local to trigger the experiments. 

