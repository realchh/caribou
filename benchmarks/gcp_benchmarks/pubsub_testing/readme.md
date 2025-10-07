# Image Processing Benchmark

Original source: https://github.com/ddps-lab/serverless-faas-workbench (the original repository's license file is included in this directory).

This benchmark measures the transmission latency for a simple 2-stage function in GCP. 

This benchmark requires setting between a large (1024 kb) and a small (10kb) message size in the input argument `message_size`.

You can deploy the benchmark with the following command while inside the poetry environment:

```bash
caribou deploy
```

And then run the benchmark with the following command:

```bash
caribou run pubsub_testing-version_number -a '{"message_size": "small"}'
```

To remove the benchmark, you can use the following command:

```bash
caribou remove pubsub_testing-version_number
```
