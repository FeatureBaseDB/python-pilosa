# Tracing

Python-Pilosa supports distributed tracing via the [OpenTracing](https://opentracing.io/) API.

In order to use a tracer with Python-Pilosa, you should:
1. Create the tracer,
2. Pass the `tracer=tracer_object` to `Client()`.

In this document, we will be using the [Jaeger](https://www.jaegertracing.io) tracer, but OpenTracing has support for [other tracing systems](https://opentracing.io/docs/supported-tracers/).

## Running the Pilosa Server

Let's run a temporary Pilosa container:

    $ docker run -it --rm -p 10101:10101 pilosa/pilosa:v1.2.0

Check that you can access Pilosa:

    $ curl localhost:10101
    Welcome. Pilosa is running. Visit https://www.pilosa.com/docs/ for more information.

## Running the Jaeger Server

Let's run a Jaeger Server container:

    $ docker run -it --rm -p 6831:6831/udp -p 5775:5775/udp -p 16686:16686 jaegertracing/all-in-one:latest
    ...<title>Jaeger UI</title>...

## Writing the Sample Code

The sample code depdends on the Jaeger Python client, so let's install it first:

    $ pip install jaeger-client

Save the following sample code as `pypilosa-tracing.py`:
```python
from pilosa import Client
from jaeger_client import Config


def get_tracer(service_name):
    config = Config(
        config={
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'local_agent': {
                'reporting_host': '127.0.0.1'
            }
        },
        service_name=service_name
    )
    return config.new_tracer()


def main():
    # Create the tracer.
    tracer = get_tracer("python_client_test")
    
    # Create the client, and pass the tracer.
    client = Client(":10101", tracer=tracer)

    # Read the schema from the server.
    # This should create a trace on the Jaeger server.
    schema = client.schema()

    # Create and sync the sample schema.
    # This should create a trace on the Jaeger server.
    my_index = schema.index("my-index")
    my_field = my_index.field("my-field")
    client.sync_schema(schema)
    
    # Run a query on Pilosa.
    # This should create a trace on the Jaeger server.
    client.query(my_field.set(1, 1000))

    tracer.close()


if __name__ == "__main__":
    main()
```

## Checking the Tracing Data

Run the sample code:

    $ python pypilosa-tracing.py

* Open http://localhost:16686 in your web browser to visit Jaeger UI.
* Click on the *Search* tab and select `python_pilosa_test` in the *Service* dropdown on the right.
* Click on *Find Traces* button at the bottom left.
* You should see a couple of traces, such as: `Client.Query`, `Client.CreateField`, `Client.Schema`, etc.
